import asyncio
import aiokafka
import datetime
import os
import subprocess
import json
from shellpaser import ServerInfo, GameDB, FlowLog, FlowPurgeLog, PacketWatchDog


async def crawl(rawQueue, durationQueue):
    proc = subprocess.Popen(['./json_capture.sh'], stdout=subprocess.PIPE)
    serverInfo = ServerInfo().get_location()
    gameDB = GameDB()
    activeWatchDog = {}
    activeFlow = {}

    while True:

        line = proc.stdout.readline().decode('utf-8').strip()
        try:
            line = dict(json.loads(line))
        except json.decoder.JSONDecodeError:
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
            if packetWatchdog.isTimeToSave() or packetWatchdog.isEndofDay():
                # packetWatchdog.save()
                data = json.dumps(packetWatchdog.getDataForSave()).encode('utf-8')
                await durationQueue.put(data)
                await asyncio.sleep(0.05)
                print(f"saved {packetWatchdog.getDataForSave()}")
                del activeWatchDog[key]


async def message_send(serverInfo, title, queue):
    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers='ec2-3-34-72-6.ap-northeast-2.compute.amazonaws.com:29092')
    try:

        # Japan_141.147.190.169_{raw or duration}
        topic = "_".join(['-'.join(serverInfo['country'].split(' ')), serverInfo['ip'], title])
        await producer.start()
        while True:
            data = await queue.get()
            await producer.send_and_wait(topic, data)
            #print(f'send', topic, data)
    finally:
        await producer.stop()


async def main():
    raw = asyncio.Queue()
    duration = asyncio.Queue()
    serverInfo = ServerInfo().get_location()
    while True:
        await asyncio.gather(*[crawl(raw, duration),
                               message_send(serverInfo, 'raw', raw),
                               message_send(serverInfo, 'duration', duration)])


if __name__ == "__main__":
    asyncio.run(main())