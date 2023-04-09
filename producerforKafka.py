import asyncio
import aiokafka
import datetime
import os
import subprocess
import json
from shellpaser import ServerInfo, GameDB, FlowLog, FlowPurgeLog, PacketWatchDog, PaymentChecker
import traceback
from collections import defaultdict


def default_factory():
    return None


async def crawl(rawQueue, durationQueue, paymentQueue):
    # proc = subprocess.Popen(['./json_capture.sh'], stdout=subprocess.PIPE)
    # 프로세스 실행 및 출력과 오류 캡처
    proc = subprocess.Popen(
        ['./json_capture.sh'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 프로세스의 출력 및 오류 얻기
    stdout, stderr = proc.communicate()
    # 프로세스의 종료 코드 얻기
    return_code = proc.returncode
    if return_code == 0:
        print("프로세스 정상 종료")
        print("프로세스 출력:\n", stdout.decode('utf-8'))
    else:
        print("프로세스 에러 발생 (종료 코드: {})".format(return_code))
        print("프로세스 오류:\n", stderr.decode('utf-8'))
    # 에러 로그 기록
    with open('error.log', 'w') as f:
    try:
        subprocess.check_call(['./json_capture.sh'], stderr=f)
    except subprocess.CalledProcessError as e:
        # handle subprocess error
        print(e)
        exit(1)
        
    serverInfo = ServerInfo().get_location()
    gameDB = GameDB()

    activeWatchDog = {}
    activeFlow = {}
    paymentWatch = {}
    localIPRecentGame = defaultdict(default_factory)
    while True:
        try:
            gameDB.updateGameDB()
            line = proc.stdout.readline().decode('utf-8').strip()

            # 파싱되는 데이터 처리
            try:
                line = dict(json.loads(line))
            except Exception as e:
                continue

            # Wg0에 flow인 데이터만 받음
            flow = FlowLog(line, serverInfo['ip'], serverInfo['country'])
            if flow.isWg0FlowFormat():
                # print('flow', len(activeFlow))
                flow.parseData()
                flow.getGameInfo(gameDB)
                flow.reformatTime()
                activeFlow[flow.getDigest()] = flow

            # 후속 데이터 병합 처리
            purgeFlow = FlowPurgeLog(line)
            if not purgeFlow.isWg0FlowPurgeFormat(): continue
            purgeFlow.parseData()
            if not purgeFlow.getDigest() in activeFlow: continue
            flow = activeFlow.pop(purgeFlow.getDigest())
            flow.insertPurgeData(purgeFlow.getFlowPurgeData())

            for key, paymentChecker in list(paymentWatch.items()):
                if paymentChecker.status != 'None':
                    if localIPRecentGame[flow.getLocalIP()] is not None:
                        paymentChecker.save(flow.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp()))
                        data = json.dumps(paymentChecker.getDataForSave(
                            flow.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp()))).encode('utf-8')
                        print('payment in queue', data)

                        await paymentQueue.put(data)
                        await asyncio.sleep(0.05)

                if paymentChecker.isTimeToWatchEnd():
                    del paymentWatch[key]
                    print('del paymentWatch', key)

            for key, packetWatchdog in list(activeWatchDog.items()):
                if packetWatchdog.isTimeToSave():
                    # packetWatchdog.save()
                    data = json.dumps(packetWatchdog.getDataForSave()).encode('utf-8')
                    await durationQueue.put(data)
                    await asyncio.sleep(0.05)
                    print(f"saved {packetWatchdog.getDataForSave()}")
                    del activeWatchDog[key]


            # Raw 데이터 처리
            if not flow.hasLocalIP(): continue
            if flow.resultData['detected_protocol_name'] in ['BitTorrent']: continue
            if flow.resultData['host_server_name'] in ['NULL', 'gateway.icloud.com', 'one.one.one.one', 'dns.quad9.net', 'app-measurement.com', 'analytics.query.yahoo.com', 'ocsp2.apple.com', 'www.googletagmanager.com', 'www.google-analytics.com']:
                continue
            data = json.dumps(flow.resultData).encode('utf-8')
            # Raw 데이터 DB에 안 들어가고 싶다면 아래 주석 처리
            # await rawQueue.put(data)
            # await paymentQueue.put(
            #     b'{"time": "2022-11-24 07:28:32.346", "server_ip": "146.56.145.179", "country": "South Korea", "local_ip": "10.0.0.5", "platform": "ios", "recentGame": "Roblox", "payment": "Trying", "host_name_server": "apis.roblox.com"}'
            # )
            await asyncio.sleep(0.05)

            # Payment 데이터 처리
            if flow.getLocalIP() not in paymentWatch:
                paymentWatch[flow.getLocalIP()] = PaymentChecker(flow.getLocalIP())
                paymentWatch[flow.getLocalIP()].setServerIp(serverInfo['ip'])
                paymentWatch[flow.getLocalIP()].setCountry(serverInfo['country'])

            paymentWatch[flow.getLocalIP()].pipe(flow.getHost_server_name())
            paymentWatch[flow.getLocalIP()].setRecentGame(localIPRecentGame[flow.getLocalIP()])
            print("Payment Watching...", flow.getLocalIP(), localIPRecentGame[flow.getLocalIP()],
                  paymentWatch[flow.getLocalIP()].status, flow.getHost_server_name())


            # Duration 데이터 처리
            if flow.getWatchKey() == 'NULL': continue
            if flow.getWatchKey() not in activeWatchDog:
                activeWatchDog[flow.getWatchKey()] = PacketWatchDog(flow.getLocalIP(),
                                                                    gameDB.getWatchTime(flow.getWatchKey()))

            activeWatchDog[flow.getWatchKey()].addPacket(*flow.getHost_server_nameAndOther_ip(),
                                                         datetime.datetime.now().timestamp(),
                                                         flow.getGame(), flow.getGameCompany(),
                                                         flow.getBytes(), flow.getPackets())
            localIPRecentGame[flow.getLocalIP()] = flow.getGame()





        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("Crawl Error : ", message)


async def message_send(serverInfo, title, queue):
    while True:
        producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=['127.0.0.1:29092',
                               '127.0.0.1:29093',
                               '127.0.0.1:29094'
                               ], acks=1)
        # producer = aiokafka.AIOKafkaProducer(bootstrap_servers='3.34.72.6:29092', acks=0)
        print(f"{title} producer start")
        try:
            # Japan_141.147.190.169_{raw or duration}
            topic = "_".join(['-'.join(serverInfo['country'].split(' ')), serverInfo['ip'], title])
            await producer.start()
            while True:
                data = await queue.get()
                await producer.send(topic, data)
                print(f'send', topic, data)
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
    payment = asyncio.Queue()
    serverInfo = ServerInfo().get_location()
    while True:
        try:
            await asyncio.gather(*[crawl(raw, duration, payment),
                                   message_send(serverInfo, 'raw', raw),
                                   message_send(serverInfo, 'duration', duration),
                                   message_send(serverInfo, 'payment', payment)
                                   ])
        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("asyncio Error : ", message)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
