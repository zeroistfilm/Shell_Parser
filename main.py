import os
import subprocess
import json
from csv import DictWriter
import datetime


class ShellLog:
    def __init__(self, data):
        self.data = data
        self.parseKey = ['detected_protocol_name',
                         'host_server_name',
                         'dns_host_name',
                         'local_ip',
                         'other_ip',
                         'local_port',
                         'other_port',
                         'first_seen_at',
                         'first_update_at',
                         'last_seen_at',
                         ]

        self.resultData = {}

    def isWg0Format(self):
        if 'interface' in self.data.keys():
            if self.data['interface'] == "wg0":
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


    def reformatTime(self):
        for key in ['first_seen_at', 'first_update_at', 'last_seen_at']:
            if type(self.resultData[key]) == datetime.datetime:
                self.resultData[key] = self.resultData[key].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    def getOtherIP(self):
        return self.resultData['other_ip']

    def getLocalIP(self):
        return self.resultData['local_ip']

    def hasLocalIP(self):
        if self.resultData['local_ip'] != 'NULL':
            return True
        else:
            return False

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

    def getFilename(self):
        return f"./csv/{self.resultData['local_ip']}.csv"

    def save(self):
        with open(self.getFilename(), 'a', newline='') as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames=self.parseKey)
            if os.path.getsize(self.getFilename()) == 0:
                dictwriter_object.writeheader()
            dictwriter_object.writerow(self.resultData)
            f_object.close()

    def __str__(self):
        return f"{self.resultData}"


class PacketWatchDog:
    # 30분 이내 2개 이상의 패킷만 저장.
    # duration = 마지막 패킷 시간 - 처음 패킷 시간
    # 필터링 조건 :  IP or DNS
    # CSV columns: date, host_server_name, other_ip, duration
    # 파일명 local_ip.csv
    # 날짜 변경을 기준으로 짜르기

    def __init__(self, watch_ip, local_ip):
        self.watch_ip = watch_ip
        self.local_ip = local_ip
        self.MIN_WATCH_COUNT = 2
        self.WATCH_TIME_MINUTES = 30
        self.DESTINATION_FILTER = ['IP', 'DNS', 'others....']
        self.CSV_COLUMNS = ['date', 'host_server_name', 'other_ip', 'duration']
        self.FILENAME = f"./csv/duration/{self.local_ip}.csv"

        self.host_server_name = 'NULL'
        self.other_ip = 'NULL'

        self.watchStart = self.getTimeKSTFromTimeStamp(datetime.datetime.now().timestamp())
        self.watchEnd = self.getTimeKSTFromTimeStamp(
            (datetime.datetime.now() + datetime.timedelta(minutes=self.WATCH_TIME_MINUTES)).timestamp())
        self.packetTimeList = []


    def getTimeKSTFromTimeStamp(self, timestamp):
        from datetime import timezone
        utctime = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")
        kstime = datetime.datetime.now().strftime("%Y%m%d_%H:%M:%S")

        # .strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        if utctime == kstime:
            return datetime.datetime.fromtimestamp(timestamp) + datetime.timedelta(hours=9)
        else:
            return datetime.datetime.fromtimestamp(timestamp)

    def addPacket(self, host_server_name, other_ip, packetTime):
        if host_server_name not in self.DESTINATION_FILTER and other_ip not in self.DESTINATION_FILTER:
            self.host_server_name = host_server_name
            self.other_ip = other_ip
            appendedPacketTime = self.getTimeKSTFromTimeStamp(packetTime)
            self.packetTimeList.append(appendedPacketTime)

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
        return {'date': self.watchStart.strftime('%Y-%m-%d'),
                'host_server_name': self.host_server_name,
                'other_ip': self.other_ip,
                'duration': round(float(self.calcDuration().total_seconds()), 4)}

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

    activeWatchDog = {}
    while True:
        line = proc.stdout.readline().decode('utf-8').strip()
        if not line:
            break

        try:
            line = dict(json.loads(line))
        except json.decoder.JSONDecodeError:
            continue

        # Save packet data
        shelllog = ShellLog(line)
        if shelllog.isWg0Format():
            shelllog.parseData()
            shelllog.reformatTime()

            if shelllog.hasLocalIP():
                shelllog.save()

            # Packet WatchDog
            if shelllog.getOtherIP() not in activeWatchDog:
                activeWatchDog[shelllog.getOtherIP()] = PacketWatchDog(shelllog.getOtherIP(), shelllog.getLocalIP())

            activeWatchDog[shelllog.getOtherIP()].addPacket(*shelllog.getHost_server_nameAndOther_ip(), datetime.datetime.now().timestamp())

            for key, packetWatchdog in list(activeWatchDog.items()):
                if packetWatchdog.isTimeToSave() or packetWatchdog.isEndofDay():
                    packetWatchdog.save()
                    print(f"saved {packetWatchdog.getDataForSave()}")
                    del activeWatchDog[key]


