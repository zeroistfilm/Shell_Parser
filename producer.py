import asyncio
import aiokafka
import datetime
import os
import subprocess
import json
from shellpaser import ServerInfo, GameDB, FlowLog, FlowPurgeLog, PacketWatchDog
import traceback


async def crawl(rawQueue, durationQueue):
    proc = subprocess.Popen(['./json_capture.sh'], stdout=subprocess.PIPE)
    serverInfo = ServerInfo().get_location()
    gameDB = GameDB()
    activeWatchDog = {}
    activeFlow = {}

    while True:
        try:
            gameDB.updateGameDB()
            line = proc.stdout.readline().decode('utf-8').strip()
            try:
                line = dict(json.loads(line))
            except Exception as e:
                continue

            flow = FlowLog(line, serverInfo['ip'], serverInfo['country'])
            if flow.isWg0FlowFormat():
                flow.parseData()
                flow.getGameInfo(gameDB)
                flow.reformatTime()
                activeFlow[flow.getDigest()] = flow

            purgeFlow = FlowPurgeLog(line)
            if purgeFlow.isWg0FlowPurgeFormat():
                purgeFlow.parseData()
                if purgeFlow.getDigest() in activeFlow:
                    flow = activeFlow.pop(purgeFlow.getDigest())
                    flow.insertPurgeData(purgeFlow.getFlowPurgeData())

                    if flow.hasLocalIP():
                        data = json.dumps(flow.resultData).encode('utf-8')
                        await rawQueue.put(data)

                        await asyncio.sleep(0.05)

                        # Packet WatchDog
                    if flow.getWatchKey() != 'NULL':

                        if flow.getWatchKey() not in activeWatchDog:
                            activeWatchDog[flow.getWatchKey()] = PacketWatchDog(flow.getLocalIP(),
                                                                                gameDB.getWatchTime(flow.getWatchKey()))

                        activeWatchDog[flow.getWatchKey()].addPacket(*flow.getHost_server_nameAndOther_ip(),
                                                                     datetime.datetime.now().timestamp(),
                                                                     flow.getGame(), flow.getGameCompany(),
                                                                     flow.getBytes(), flow.getPackets())

                        print('add packet', *flow.getHost_server_nameAndOther_ip(),
                              datetime.datetime.now().timestamp(),
                              flow.getGame(), flow.getGameCompany(),
                              flow.getBytes(), flow.getPackets())

            for key, packetWatchdog in list(activeWatchDog.items()):
                if packetWatchdog.isTimeToSave():
                    # packetWatchdog.save()
                    data = json.dumps(packetWatchdog.getDataForSave()).encode('utf-8')

                    await durationQueue.put(data)

                    await asyncio.sleep(0.05)
                    print(f"saved {packetWatchdog.getDataForSave()}")
                    del activeWatchDog[key]
        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("Crawl Error : ",message)

async def message_send(serverInfo, title, queue):
    while True:
        #producer = aiokafka.AIOKafkaProducer(bootstrap_servers='ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29092',acks=0)
        producer = aiokafka.AIOKafkaProducer(bootstrap_servers='3.34.72.6:29092', acks=0)
        print(f"{title} producer start")
        try:
            # Japan_141.147.190.169_{raw or duration}
            topic = "_".join(['-'.join(serverInfo['country'].split(' ')), serverInfo['ip'], title])
            await producer.start()
            while True:
                data = await queue.get()
                await producer.send(topic, data)
                #print(f'send', topic, data)
        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("kafka producer Error : ", message)

        finally:
            await producer.stop()
            print(f"{title} producer end")

async def main():
    raw = asyncio.Queue()
    duration = asyncio.Queue()
    serverInfo = ServerInfo().get_location()
    while True:
        try:
            await asyncio.gather(*[crawl(raw, duration),
                                   message_send(serverInfo, 'raw', raw),
                                   message_send(serverInfo, 'duration', duration)])
        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("asyncio Error : ", message)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
