import asyncio
import random
import aiokafka
import datetime
import os
import subprocess
from shellpaser import (
    ServerInfo,
    GameDB,
    FlowLog,
    FlowPurgeLog,
    PacketWatchDog,
    PaymentChecker,
)
import traceback
from collections import defaultdict
import time

import socket
import json


def default_factory():
    return None


class KillChecker:
    def __init__(self):
        self.lastInput = None
        self.lastInputTime = None

    def isKillCondition(self, data):
        current_time = time.time()
        # print(self.lastInput, data, self.lastInputTime, current_time)
        if self.lastInputTime is None:
            self.lastInputTime = current_time
        elif self.lastInput == data and current_time - self.lastInputTime >= 10:
            print("no data with 10sec")
            try:
                process_name = "sudo nc -U /var/run/netifyd/netifyd.sock"
                # ps -ef 명령어 실행
                result = subprocess.run(
                    ["ps", "-ef"], stdout=subprocess.PIPE, text=True
                )
                # 결과 파싱하여 특정 프로세스(process_name)가 있는지 확인
                for line in result.stdout.splitlines():
                    if process_name in line:
                        print("nc found")
                        return False
                print("nc not found")
                return True
            except Exception as e:
                print(f"Error: {e}")
                return True

        elif self.lastInput != data:
            self.lastInput = data
            self.lastInputTime = current_time


async def async_socket_reader(sock_file_path):
    reader, writer = await asyncio.open_unix_connection(sock_file_path)
    return reader


async def crawl(rawQueue, durationQueue, paymentQueue):
    try:
        # proc = subprocess.Popen(['./json_capture.sh'], stdout=subprocess.PIPE)
        print("start crawl")
        serverInfo = ServerInfo().get_location()
        gameDB = GameDB()
        activeWatchDog = {}
        activeFlow = {}
        paymentWatch = {}
        localIPRecentGame = defaultdict(default_factory)
        killchecker = KillChecker()
        sock_file_path = "/var/run/netifyd/netifyd.sock"
        reader = await async_socket_reader(sock_file_path)

        while True:
            gameDB.updateGameDB()
            data = await reader.read(100000)
            if not data:
                break
            decoded_string = data.decode("utf-8")
            split_strings = decoded_string.split("\n")
            del data

            for string in split_strings:
                try:
                    line = dict(json.loads(string))
                except Exception as e:
                    continue

                # print(line)
                # Wg0에 flow인 데이터만 받음
                flow = FlowLog(line, serverInfo["ip"], serverInfo["country"])
                if flow.isWg0FlowFormat():
                    # print('flow', len(activeFlow))
                    flow.parseData()
                    flow.getGameInfo(gameDB)
                    flow.reformatTime()
                    activeFlow[flow.getDigest()] = flow

                # 후속 데이터 병합 처리
                purgeFlow = FlowPurgeLog(line)
                if not purgeFlow.isWg0FlowPurgeFormat():
                    continue
                purgeFlow.parseData()
                if not purgeFlow.getDigest() in activeFlow:
                    continue
                flow = activeFlow.pop(purgeFlow.getDigest())
                flow.insertPurgeData(purgeFlow.getFlowPurgeData())

                for key, paymentChecker in list(paymentWatch.items()):
                    if paymentChecker.status != "None":
                        if localIPRecentGame[flow.getLocalIP()] is not None:
                            paymentChecker.save(
                                flow.getTimeKSTFromTimeStamp(
                                    datetime.datetime.now().timestamp()
                                )
                            )
                            data = json.dumps(
                                paymentChecker.getDataForSave(
                                    flow.getTimeKSTFromTimeStamp(
                                        datetime.datetime.now().timestamp()
                                    )
                                )
                            ).encode("utf-8")
                            print("payment in queue", data)

                            await paymentQueue.put(data)
                            await asyncio.sleep(0.05)

                    if paymentChecker.isTimeToWatchEnd():
                        del paymentWatch[key]
                        print("del paymentWatch", key)

                for key, packetWatchdog in list(activeWatchDog.items()):
                    if packetWatchdog.isTimeToSave():
                        # packetWatchdog.save()
                        data = json.dumps(packetWatchdog.getDataForSave()).encode(
                            "utf-8"
                        )
                        await durationQueue.put(data)
                        await asyncio.sleep(0.05)
                        print(f"saved {packetWatchdog.getDataForSave()}")
                        del activeWatchDog[key]

                # Raw 데이터 처리
                if not flow.hasLocalIP():
                    continue
                if flow.resultData["detected_protocol_name"] in ["BitTorrent"]:
                    continue
                if flow.resultData["host_server_name"] in [
                    "NULL",
                    "gateway.icloud.com",
                    "one.one.one.one",
                    "dns.quad9.net",
                    "app-measurement.com",
                    "analytics.query.yahoo.com",
                    "ocsp2.apple.com",
                    "www.googletagmanager.com",
                    "www.google-analytics.com",
                ]:
                    continue
                data = json.dumps(flow.resultData).encode("utf-8")
                # Raw 데이터 DB에 안 들어가고 싶다면 아래 주석 처리
                # print(data)
                # await rawQueue.put(data)
                # await asyncio.sleep(0.05)

                # Payment 데이터 처리
                if flow.getLocalIP() not in paymentWatch:
                    paymentWatch[flow.getLocalIP()] = PaymentChecker(flow.getLocalIP())
                    paymentWatch[flow.getLocalIP()].setServerIp(serverInfo["ip"])
                    paymentWatch[flow.getLocalIP()].setCountry(serverInfo["country"])

                paymentWatch[flow.getLocalIP()].pipe(flow.getHost_server_name())
                paymentWatch[flow.getLocalIP()].setRecentGame(
                    localIPRecentGame[flow.getLocalIP()]
                )
                print(
                    "Payment Watching...",
                    flow.getLocalIP(),
                    localIPRecentGame[flow.getLocalIP()],
                    paymentWatch[flow.getLocalIP()].status,
                    flow.getHost_server_name(),
                )

                # Duration 데이터 처리
                if flow.getWatchKey() == "NULL":
                    continue
                if flow.getWatchKey() not in activeWatchDog:
                    activeWatchDog[flow.getWatchKey()] = PacketWatchDog(
                        flow.getLocalIP(), gameDB.getWatchTime(flow.getWatchKey())
                    )

                activeWatchDog[flow.getWatchKey()].addPacket(
                    *flow.getHost_server_nameAndOther_ip(),
                    datetime.datetime.now().timestamp(),
                    flow.getGame(),
                    flow.getGameCompany(),
                    flow.getBytes(),
                    flow.getPackets(),
                )
                localIPRecentGame[flow.getLocalIP()] = flow.getGame()

    except Exception as e:
        reader.close()
        trace_back = traceback.format_exc()
        message = str(e) + "\n" + str(trace_back)
        print("Crawl Error : ", message)
        raise Exception("Crawl Error")


async def message_send(serverInfo, title, queue):
    if serverInfo["ip"] == "152.70.249.8":
        kafkaIp = "127.0.0.1"
    else:
        kafkaIp = "152.70.249.8"
    print(kafkaIp)
    while True:
        producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=[
                f"{kafkaIp}:29092",
                f"{kafkaIp}:29093",
                f"{kafkaIp}:29094",
            ],
            acks=1,
        )
        # producer = aiokafka.AIOKafkaProducer(bootstrap_servers='3.34.72.6:29092', acks=0)
        print(f"{title} producer start")
        try:
            # Japan_141.147.190.169_{raw or duration}
            topic = "_".join(
                ["-".join(serverInfo["country"].split(" ")), serverInfo["ip"], title]
            )
            await producer.start()
            while True:
                data = await queue.get()
                await producer.send(topic, data)
                print(f"send", topic, data)
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
            tasks = [
                crawl(raw, duration, payment),
                message_send(serverInfo, "raw", raw),
                message_send(serverInfo, "duration", duration),
                message_send(serverInfo, "payment", payment),
            ]

            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_EXCEPTION
            )

        except Exception as e:
            trace_back = traceback.format_exc()
            message = str(e) + "\n" + str(trace_back)
            print("asyncio Error : ", message)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
