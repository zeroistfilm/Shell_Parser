import os
import subprocess
import json
from csv import DictWriter
import datetime
import requests


class ServerInfo:
    def get_ip(self):
        response = requests.get('https://api.ipify.org?format=json').json()
        return response["ip"]

    def get_location(self):
        ip_address = self.get_ip()
        response = requests.get(f'https://ipapi.co/{ip_address}/json/').json()
        location_data = {
            "ip": ip_address,
            "city": response.get("city"),
            "region": response.get("region"),
            "country": response.get("country_name")
        }
        return location_data


class GameDB:
    def __init__(self):
        self.gameDB = {}
        self.gameWatchTime = {}
        self.gameDBPath = 'game_db.csv'

        self.loadGameDB()

    def getGameDB(self):
        return self.gameDB

    def getWatchTime(self, game):
        if game == 'NULL':
            return 0
        return int(self.gameWatchTime[game])

    def loadGameDB(self):
        with open(self.gameDBPath, "rt",encoding='UTF8') as f:
            gameList = f.read().splitlines()
            for game in gameList:
                # key parsing
                try:
                    parseData = [i.strip() for i in game.split(',')]
                    if len(parseData) < 5:
                        continue
                    if len(parseData) >= 5:
                        watchTime, game_company, game, host_server_name, *other_ip = parseData
                        other_ip = [i.strip() for i in other_ip if i.strip()]
                        if watchTime == '' or game_company == '' or game == '' or host_server_name == '':
                            continue


                except Exception as e:
                    continue
                print(watchTime, game_company, game, host_server_name, other_ip)

                # set gameDB
                if len(other_ip) == 1:
                    if other_ip[0] == 'NULL':
                        if not host_server_name == 'NULL':
                            self.gameDB[host_server_name] = {'game_company': game_company, 'game': game}

                    self.gameDB[other_ip[0]] = {'game_company': game_company, 'game': game}

                elif len(other_ip) > 1:
                    for ip in other_ip:
                        self.gameDB[ip] = {'game_company': game_company, 'game': game}

                if not host_server_name == 'NULL':
                    self.gameDB[host_server_name] = {'game_company': game_company, 'game': game}

                self.gameWatchTime[game] = int(watchTime)

            try:
                del self.gameDB['NULL']
                del self.gameDB['0.0.0.0']
            except Exception as e:
                print(e)


        for key in self.gameDB.keys():
            print(key, self.gameDB[key])


    def getWildCard(self, host_server_name):
        if host_server_name == 'NULL':
            return 'NULL'
        else:
            return '.'.join(map(str, ['*', *host_server_name.split('.')[-2:]]))


class FlowLog:
    def __init__(self, data, server_ip, country):
        self.server_ip = server_ip
        self.country = country
        self.data = data
        self.parseKey = ['server_ip',
                         'country',
                         'detected_application_name',
                         'detected_protocol_name',
                         'host_server_name',
                         'dns_host_name',
                         'local_ip',
                         'local_port',
                         'other_ip',
                         'other_port',
                         'first_seen_at',
                         'first_update_at',
                         'last_seen_at',
                         'game',
                         'game_company',
                         'digest',
                         'local_bytes',
                         'local_packets',
                         'other_bytes',
                         'other_packets',
                         'total_bytes',
                         'total_packets']

        self.resultData = {}

        # print(self.resultData)

    def getGameInfo(self, gameDB):
        hostServerName = self.resultData['host_server_name']
        otherIP = self.resultData['other_ip']
        whilCard = gameDB.getWildCard(hostServerName)


        if hostServerName != 'NULL':
            if hostServerName in gameDB.getGameDB():
                self.resultData['game_company'] = gameDB.getGameDB()[hostServerName]['game_company']
                self.resultData['game'] = gameDB.getGameDB()[hostServerName]['game']
            elif whilCard in gameDB.getGameDB():
                self.resultData['game_company'] = gameDB.getGameDB()[whilCard]['game_company']
                self.resultData['game'] = gameDB.getGameDB()[whilCard]['game']

        else:  # hostServerName == 'NULL'
            if otherIP in gameDB.getGameDB():
                self.resultData['game_company'] = gameDB.getGameDB()[otherIP]['game_company']
                self.resultData['game'] = gameDB.getGameDB()[otherIP]['game']

    def isWg0FlowFormat(self):
        if 'interface' in self.data.keys():
            if self.data['interface'] == "wg0":
                if self.data['type'] == 'flow':
                    return True
        return False

    def parseData(self):
        self.data = self.data['flow']
        for key in self.parseKey:
            try:
                if key.split('_')[-1] == "at":
                    self.resultData[key] = self.getTimeKSTFromTimeStamp(int(self.data[key]) / 1000)

                else:
                    self.resultData[key] = self.data[key]
            except Exception as e:
                self.resultData[key] = 'NULL'

        self.resultData['local_ip'] = self.convertIPv4(self.resultData['local_ip'])
        self.resultData['server_ip'] = self.server_ip
        self.resultData['country'] = self.country

    def convertIPv4(self, ip):

        if len(ip.split('.')) == 4:
            return ip

        if len(ip.split(':')) == 6:


            number = int(ip.split(':')[-1], base=16)
            retval = [str(number >> i & 0xFF) for i in (24, 16, 8, 0)]
            retval[0] = '10'

            print('==============================',ip, '.'.join(retval),'==============================')
            return '.'.join(retval)

    def reformatTime(self):
        for key in ['first_seen_at', 'first_update_at', 'last_seen_at']:
            if type(self.resultData[key]) == datetime.datetime:
                self.resultData[key] = self.resultData[key].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    def getOtherIP(self):
        return self.resultData['other_ip']

    def getLocalIP(self):
        return self.resultData['local_ip']

    def getDigest(self):
        return self.resultData['digest']

    def hasLocalIP(self):
        if self.resultData['local_ip'] != 'NULL':
            return True
        else:
            return False

    def getGame(self):
        return self.resultData['game']

    def getGameCompany(self):
        return self.resultData['game_company']

    def getBytes(self):
        return self.resultData['total_bytes']

    def getPackets(self):
        return self.resultData['total_packets']

    def getTimeKSTFromTimeStamp(self, timestamp):
        from datetime import timezone
        utctime = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")
        kstime = datetime.datetime.now().strftime("%Y%m%d_%H:%M:%S")

        # .strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        if utctime == kstime:
            return datetime.datetime.fromtimestamp(timestamp) + datetime.timedelta(hours=9)
        else:
            return datetime.datetime.fromtimestamp(timestamp)

    def getHost_server_nameAndOther_ip(self):
        return self.resultData['host_server_name'], self.resultData['other_ip']

    def getLastSeenAtDatetime(self):
        if type(self.resultData['last_seen_at']) == datetime.datetime:
            return self.resultData['last_seen_at']
        elif type(self.resultData['last_seen_at']) == str:
            return datetime.datetime.strptime(self.resultData['last_seen_at'], '%Y-%m-%d %H:%M:%S.%f')

    def getWatchKey(self):
        return self.resultData['game']

    def getFilename(self):
        return f"./csv/{self.resultData['local_ip']}.csv"

    def insertPurgeData(self, purgeData):
        self.resultData['local_bytes'] = purgeData['local_bytes']
        self.resultData['local_packets'] = purgeData['local_packets']
        self.resultData['other_bytes'] = purgeData['other_bytes']
        self.resultData['other_packets'] = purgeData['other_packets']
        self.resultData['total_bytes'] = purgeData['total_bytes']
        self.resultData['total_packets'] = purgeData['total_packets']

    def save(self):
        print(self.resultData)
        with open(self.getFilename(), 'a', newline='') as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=self.parseKey)
            if os.path.getsize(self.getFilename()) == 0:
                dictwriter_object.writeheader()
            dictwriter_object.writerow(self.resultData)
            f_object.close()

    def __str__(self):
        return f'local_ip:{self.resultData["local_ip"]} game_company: {self.resultData["game_company"]}, game: {self.resultData["game"]}, host_name_server: {self.resultData["host_server_name"]}'


class FlowPurgeLog:

    # flow_purge???  local_bytes, local_packets, other_bytes, other_packets, total_bytes, total_packets ??? ??????csv??? ??????
    def __init__(self, data):
        self.data = data
        self.parseKey = ['digest',
                         'local_bytes',
                         'local_packets',
                         'other_bytes',
                         'other_packets',
                         'total_bytes',
                         'total_packets'
                         ]
        self.resultData = {}

    def isWg0FlowPurgeFormat(self):
        if 'interface' in self.data.keys():
            if self.data['interface'] == "wg0":
                if self.data['type'] == 'flow_purge':
                    return True
        return False

    def parseData(self):
        self.data = self.data['flow']
        for key in self.parseKey:
            try:
                self.resultData[key] = self.data[key]
            except Exception as e:
                self.resultData[key] = 'NULL'

    def getDigest(self):
        return self.resultData['digest']

    def getFilename(self):
        return f"./csv/{self.resultData['local_ip']}.csv"

    def getFlowPurgeData(self):
        return self.resultData

    def save(self):

        with open(self.getFilename(), 'a', newline='') as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=self.parseKey)
            if os.path.getsize(self.getFilename()) == 0:
                dictwriter_object.writeheader()
            dictwriter_object.writerow(self.resultData)
            f_object.close()


class PacketWatchDog:
    # 30??? ?????? 2??? ????????? ????????? ??????.
    # duration = ????????? ?????? ?????? - ?????? ?????? ??????
    # ????????? ?????? :  IP or DNS
    # CSV columns: date, host_server_name, other_ip, duration
    # ????????? local_ip.csv
    # ?????? ????????? ???????????? ?????????
    def __init__(self, local_ip, watchTimeMin):

        self.local_ip = local_ip
        self.MIN_WATCH_COUNT = 2
        self.WATCH_TIME_MINUTES = int(watchTimeMin)
        self.DESTINATION_FILTER = ['IP', 'DNS', 'others....']
        self.CSV_COLUMNS = ['server_ip', 'country', 'date', 'start_time', 'end_time', 'host_server_name', 'other_ip',
                            'duration', 'game',
                            'game_company', 'bytes', 'packets']
        self.FILENAME = f"./csv/duration/{self.local_ip}.csv"

        self.host_server_name = 'NULL'
        self.other_ip = 'NULL'

        self.watchStart = self.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp())
        self.watchEnd = self.getTimeKSTFromTimeStamp(
            (datetime.datetime.now() + datetime.timedelta(minutes=self.WATCH_TIME_MINUTES)).timestamp())

        self.packetTimeList = []
        self.game = 'NULL'
        self.game_company = 'NULL'
        self.bytesList = []
        self.packetsList = []
        print(f"watchStart: {self.watchStart}, watchEnd: {self.watchEnd}")

    def addPacket(self, host_server_name, other_ip, packetTime, game, gameCompany, eachBytes, eachPackets):
        if host_server_name not in self.DESTINATION_FILTER and other_ip not in self.DESTINATION_FILTER:
            self.host_server_name = host_server_name
            self.other_ip = other_ip
            self.game = game
            self.game_company = gameCompany

            appendedPacketTime = self.getTimeKSTFromTimeStamp(packetTime)
            self.packetTimeList.append(appendedPacketTime)
            self.bytesList.append(eachBytes)
            self.packetsList.append(eachPackets)

    def getTimeKSTFromTimeStamp(self, timestamp):
        from datetime import timezone
        utctime = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")
        kstime = datetime.datetime.now().strftime("%Y%m%d_%H:%M:%S")

        # .strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        if utctime == kstime:
            return datetime.datetime.fromtimestamp(timestamp) + datetime.timedelta(hours=9)
        else:
            return datetime.datetime.fromtimestamp(timestamp)

    def calcDuration(self):
        if self.isTimeToSave():
            return self.packetTimeList[-1] - self.watchStart

    def isTimeToSave(self):
        if self.watchEnd < self.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp()):
            return True
        else:
            return False

    def isSaveCondition(self):
        if len(self.packetTimeList) >= self.MIN_WATCH_COUNT:
            return True
        else:
            return False

    def getDataForSave(self):
        return {'server_ip': ServerInfo().get_location()['ip'],
                'country': ServerInfo().get_location()['country'],
                'date': self.watchStart.strftime('%Y-%m-%d'),
                'start_time': self.watchStart.strftime('%H:%M:%S.%f'),
                'end_time': self.packetTimeList[-1].strftime('%H:%M:%S.%f'),
                'host_server_name': self.host_server_name,
                'other_ip': self.other_ip,
                'duration': round(float(self.calcDuration().total_seconds()), 4),
                'game': self.game,  # flow?????? ????????????
                'game_company': self.game_company,  # flow?????? ????????????
                'bytes': sum(self.bytesList),  # pureflow?????? ????????? ???
                'packets': sum(self.packetsList)}  # pureflow?????? ????????? ???

    def isEndofDay(self):
        if self.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp()).hour == '23' and \
                self.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp()).minute == '59' and \
                self.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp()).second == '59':
            return True
        else:
            return False

    def save(self):
        if self.isSaveCondition():
            with open(self.FILENAME, 'a', newline='') as f_object:
                dictwriter_object = DictWriter(f_object, fieldnames=self.CSV_COLUMNS)
                if os.path.getsize(self.FILENAME) == 0:
                    dictwriter_object.writeheader()
                dictwriter_object.writerow(self.getDataForSave())
                f_object.close()


if __name__ == "__main__":
    proc = subprocess.Popen(['./json_capture.sh'], stdout=subprocess.PIPE)
    gameDB = GameDB()
    activeWatchDog = {}
    activeFlow = {}

    serverInfo = ServerInfo().get_location()

    while True:
        try:
            line = proc.stdout.readline().decode('utf-8').strip()
            if not line:
                break

            try:
                line = dict(json.loads(line))
            except json.decoder.JSONDecodeError:
                continue

            # Save packet data
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
                        flow.save()
                    print('flow', flow)

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
                    packetWatchdog.save()
                    print(f"saved {packetWatchdog.getDataForSave()}")
                    del activeWatchDog[key]

        except Exception as e:
            print(e)
            continue
        except KeyboardInterrupt:
            print('KeyboardInterrupt')
            break
