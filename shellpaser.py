import os
import subprocess
import json
from csv import DictWriter
import datetime
import requests
import time
from pytz import timezone


class ServerInfo:

    def __init__(self):
        '''
        146.56.42.103: South Korea
        54.250.113.35: Japan
        144.24.119.251: India
        129.158.221.8: United States

        마릴 VPN
        45.77.65.232: Germany
        15.206.180.184: India
        35.79.143.27: Japan
        172.107.194.178: South Korea
        104.156.250.10: United States (East)
        54.200.124.241: United States (West)


        마릴 GO 테스트 서버
        141.147.190.169: Japan
        146.56.145.179: South Korea
        '''
        self.location_dict = {  # 마릴 GO
            '146.56.42.103': {'ip': '146.56.42.103', 'country': 'South Korea'},
            '54.250.113.35': {'ip': '54.250.113.35', 'country': 'Japan'},
            '144.24.119.251': {'ip': '144.24.119.251', 'country': 'India'},
            '129.158.221.8': {'ip': '129.158.221.8', 'country': 'United States'},

            # 마릴 VPN
            '52.29.224.219': {'ip': '52.29.224.219', 'country': 'Germany'},
            '35.79.143.27': {'ip': '35.79.143.27', 'country': 'Japan'},
            '172.107.194.178': {'ip': '172.107.194.178', 'country': 'South Korea'},
            '129.159.126.80': {'ip': '129.159.126.80', 'country': 'United States'},
            '54.200.124.241': {'ip': '54.200.124.241', 'country': 'United States'},

            # 마릴 VPN 테스트 서버
            '130.162.152.89': {'ip': '130.162.152.89', 'country': 'South Korea'},

            # 마릴 GO 테스트 서버
            '141.147.190.169': {'ip': '141.147.190.169', 'country': 'Japan'},
            '146.56.145.179': {'ip': '146.56.145.179', 'country': 'South Korea'},




        }


    def get_ip(self):
        response = requests.get('https://api.ipify.org?format=json').json()
        return response["ip"]

    def get_location(self):
        # while True:
        ip_address = self.get_ip()
        #    response = requests.get(f'https://ipapi.co/{ip_address}/json/').json()
        #    location_data = {
        #        "ip": ip_address,
        #        "city": response.get("city"),
        ##        "region": response.get("region"),
        #        "country": response.get("country_name")
        #    }
        #    if location_data['country'] != None:
        location_data = self.location_dict[ip_address]
    ##        break
    #   time.sleep(1)
    #   print("retrying to get location data")


        return location_data


class GameDB:
    def __init__(self):
        self.gameDB = {}
        self.gameWatchTime = {}
        self.gameDBPath = 'game_db.csv'
        self.loadGameDB()
        self.updateInterval = 5
        self.dueUpdateTime = datetime.datetime.now() + datetime.timedelta(minutes=self.updateInterval)

    def updateGameDB(self):
        if datetime.datetime.now() > self.dueUpdateTime:
            self.now = datetime.datetime.now()
            self.loadGameDB()
            self.dueUpdateTime = self.now + datetime.timedelta(minutes=self.updateInterval)

    def getGameDB(self):
        return self.gameDB

    def getWatchTime(self, game):
        if game == 'NULL':
            return 0
        return int(self.gameWatchTime[game])

    def loadGameDB(self):
        self.gameDB = {}
        with open(self.gameDBPath, "rt", encoding='UTF8') as f:
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
                # print(watchTime, game_company, game, host_server_name, other_ip)

                # set gameDB
                # if len(other_ip) == 1:
                #     if other_ip[0] == 'NULL':
                #         if not host_server_name == 'NULL':
                #             self.gameDB[host_server_name] = {'game_company': game_company, 'game': game}
                #
                #     self.gameDB[other_ip[0]] = {'game_company': game_company, 'game': game}

                # elif len(other_ip) > 1:
                #     for ip in other_ip:
                #         self.gameDB[ip] = {'game_company': game_company, 'game': game}

                if not host_server_name == 'NULL':
                    self.gameDB[host_server_name] = {'game_company': game_company, 'game': game}

                self.gameWatchTime[game] = int(watchTime)

            try:
                del self.gameDB['NULL']
                del self.gameDB['0.0.0.0']
            except Exception as e:
                pass

        # for key in self.gameDB.keys():
        #    print(key, self.gameDB[key])

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
        self.parseKey = ['local_ip',
                         'server_ip',
                         'country',
                         'detected_application_name',
                         'detected_protocol_name',
                         'host_server_name',
                         'dns_host_name',
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
                    if self.data['detected_protocol_name'] != 'BitTorrent':
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

            #print('==============================', ip, '.'.join(retval), '==============================')
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
        return datetime.datetime.fromtimestamp(timestamp, timezone('Asia/Seoul'))

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

    # flow_purge에  local_bytes, local_packets, other_bytes, other_packets, total_bytes, total_packets 를 기존csv에 추가
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
    # 30분 이내 2개 이상의 패킷만 저장.
    # duration = 마지막 패킷 시간 - 처음 패킷 시간
    # 필터링 조건 :  IP or DNS
    # CSV columns: date, host_server_name, other_ip, duration
    # 파일명 local_ip.csv
    # 날짜 변경을 기준으로 짜르기
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
        self.watchEnd = self.getTimeKSTFromTimeStamp((datetime.datetime.now() + datetime.timedelta(minutes=self.WATCH_TIME_MINUTES)).timestamp())

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

        return datetime.datetime.fromtimestamp(timestamp, timezone('Asia/Seoul'))

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
                'local_ip': self.local_ip,
                'date': self.watchStart.strftime('%Y-%m-%d'),
                'start_time': self.watchStart.strftime('%H:%M:%S.%f'),
                'end_time': self.packetTimeList[-1].strftime('%H:%M:%S.%f'),
                'host_server_name': self.host_server_name,
                'other_ip': self.other_ip,
                'duration': round(float(self.calcDuration().total_seconds()), 4),
                'game': self.game,  # flow에서 가져오기
                'game_company': self.game_company,  # flow에서 가져오기
                'bytes': sum(self.bytesList),  # pureflow에서 가져올 것
                'packets': sum(self.packetsList)}  # pureflow에서 가져올 것

    def isEndofDay(self):
        currTime = self.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp())
        if currTime.hour == '23' and \
                currTime.minute == '59' and \
                currTime.second == '59':
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


class paymentWatchDog:
    def __init__(self):
        self.ANDROID_PAYMENT_TRY = ""
        self.ANDROID_PAYMENT_FAIL = ""
        self.ANDROID_PAYMENT_SUCCESS = ""

        self.IOS_PAYMENT_TRY = ""
        self.IOS_PAYMENT_FAIL = ""
        self.IOS_PAYMENT_SUCCESS = ""


if __name__ == "__main__":
    db = GameDB()
    db.loadGameDB()