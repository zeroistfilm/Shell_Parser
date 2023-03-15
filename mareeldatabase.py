import time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
#

localDBHost = 'mysql+mysqldb://empo:dpavh1423@10.2.1.6:3306/mareel_traffic'
remoteDBHost = 'mysql+mysqldb://empo:dpavh1423@staging-db.crtzcfwok2ed.ap-northeast-2.rds.amazonaws.com:3306/mareel_traffic'


DATABASES = create_engine(localDBHost)
# orm과의 매핑 선언
Base = declarative_base()
# 테이블 생성


class mareelDB():
    def __init__(self):

        self.DATABASES = create_engine(remoteDBHost)
        self.Base = declarative_base()
        self.Base.metadata.create_all(self.DATABASES)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.DATABASES)
        self.session = self.Session()


def createDurationTable(serviceName):
    class duration(Base):
        __tablename__ = serviceName
        __table_args__ = {'extend_existing': True}
        managed = True
        id = Column(Integer, primary_key=True)
        server_ip = Column(String(50))
        local_ip = Column(String(50))
        country = Column(String(50))
        date = Column(String(50))
        start_time = Column(String(50))
        end_time = Column(String(50))
        host_server_name = Column(String(50))
        other_ip = Column(String(50))
        duration = Column(String(50))
        game = Column(String(50))
        game_company = Column(String(50))
        bytes = Column(String(50))
        packets = Column(String(50))

        def __init__(self, data):
            self.data = data
            self.parse()

        def parse(self):
            self.data = json.loads(self.data)
            self.server_ip = self.data['server_ip']
            self.local_ip = self.data['local_ip']
            self.country = self.data['country']
            self.date = self.data['date']
            self.start_time = self.data['start_time']
            self.end_time = self.data['end_time']
            self.host_server_name = self.data['host_server_name']
            self.other_ip = self.data['other_ip']
            self.duration = self.data['duration']
            self.game = self.data['game']
            self.game_company = self.data['game_company']
            self.bytes = self.data['bytes']
            self.packets = self.data['packets']

        def __repr__(self):
            return  f"<{self.__tablename__}('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')>" % (
                self.server_ip, self.local_ip, self.country, self.date, self.start_time, self.end_time,
                self.host_server_name,
                self.other_ip, self.duration, self.game, self.game_company, self.bytes, self.packets)

    return duration


def createRawTable(serviceName):
    class raw(Base):
        __tablename__ = serviceName
        __table_args__ = {'extend_existing': True}
        managed=True
        id = Column(Integer, primary_key=True)
        local_ip = Column(String(50))
        server_ip = Column(String(50))
        country = Column(String(50))
        detected_application_name = Column(String(50))
        detected_protocol_name = Column(String(50))
        host_server_name = Column(String(50))
        dns_host_name = Column(String(50))
        local_port = Column(String(50))
        other_ip = Column(String(50))
        other_port = Column(String(50))
        first_seen_at = Column(String(50))
        first_update_at = Column(String(50))
        last_seen_at = Column(String(50))
        game = Column(String(50))
        game_company = Column(String(50))
        digest = Column(String(50))
        local_bytes = Column(String(50))
        local_packets = Column(String(50))
        other_bytes = Column(String(50))
        other_packets = Column(String(50))
        total_bytes = Column(String(50))
        total_packets = Column(String(50))

        def __init__(self, data):
            self.jsonByte = data
            self.parse()

        def parse(self):
            jsonByte = json.loads(self.jsonByte)
            self.local_ip = jsonByte['local_ip']
            self.server_ip = jsonByte['server_ip']
            self.country = jsonByte['country']
            self.detected_application_name = jsonByte['detected_application_name']
            self.detected_protocol_name = jsonByte['detected_protocol_name']
            self.host_server_name = jsonByte['host_server_name']
            self.dns_host_name = jsonByte['dns_host_name']
            self.local_port = jsonByte['local_port']
            self.other_ip = jsonByte['other_ip']
            self.other_port = jsonByte['other_port']
            self.first_seen_at = jsonByte['first_seen_at']
            self.first_update_at = jsonByte['first_update_at']
            self.last_seen_at = jsonByte['last_seen_at']
            self.game = jsonByte['game']
            self.game_company = jsonByte['game_company']
            self.digest = jsonByte['digest']
            self.local_bytes = jsonByte['local_bytes']
            self.local_packets = jsonByte['local_packets']
            self.other_bytes = jsonByte['other_bytes']
            self.other_packets = jsonByte['other_packets']
            self.total_bytes = jsonByte['total_bytes']
            self.total_packets = jsonByte['total_packets']

        def __repr__(self):
            return f"<{self.__tablename__}('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')>" % (
                self.local_ip, self.server_ip, self.country, self.detected_application_name,
                self.detected_protocol_name,
                self.host_server_name, self.dns_host_name, self.local_port, self.other_ip, self.other_port,
                self.first_seen_at,
                self.first_update_at, self.last_seen_at, self.game, self.game_company, self.digest, self.local_bytes,
                self.local_packets, self.other_bytes, self.other_packets, self.total_bytes, self.total_packets)

    return raw


def createPaymentTable(serviceName):
    class payment(Base):
        __tablename__ = serviceName
        __table_args__ = {'extend_existing': True}
        managed=True
        id = Column(Integer, primary_key=True)
        time = Column(String(50))
        country = Column(String(50))
        server_ip = Column(String(50))
        local_ip= Column(String(50))
        platform = Column(String(50))
        payment= Column(String(50))
        recentGame= Column(String(50))
        host_name_server= Column(String(50))



        def __init__(self, data):
            self.jsonByte = data
            self.parse()

        def parse(self):
            jsonByte = json.loads(self.jsonByte)
            self.time = jsonByte['time']
            self.server_ip = jsonByte['server_ip']
            self.local_ip = jsonByte['local_ip']
            self.country = jsonByte['country']
            self.platform = jsonByte['platform']
            self.payment = jsonByte['payment']
            self.recentGame = jsonByte['recentGame']
            self.host_name_server = jsonByte['host_name_server']



        def __repr__(self):
            return f"<{self.__tablename__}( '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')>" % (
                self.time, self.server_ip, self.local_ip, self.country, self.platform, self.payment, self.recentGame, self.host_name_server)

    return payment

if __name__ == '__main__':

    Session = sessionmaker()
    Session.configure(bind=DATABASES)
    session = Session()

    #raw = createRawTable('Mareel_GO_Test_Raw')
    #duration = createDurationTable('Mareel_GO_Test_Duration')
    payment = createPaymentTable('Mareel_GO_Test_Payment')
    Base.metadata.create_all(DATABASES)

    while True:
        #rawdata = b'{"local_ip": "10.0.0.5", "server_ip": "146.56.145.179", "country": "South Korea", "detected_application_name": "Unknown", "detected_protocol_name": "HTTP/S", "host_server_name": "NULL", "dns_host_name": "NULL", "local_port": 58676, "other_ip": "211.114.66.12", "other_port": 443, "first_seen_at": "2022-10-22 23:30:30.224", "first_update_at": "2022-10-22 23:30:30.224", "last_seen_at": "2022-10-22 23:30:30.224", "game": "NULL", "game_company": "NULL", "digest": "79dc2320023a6c2c1ef73e148e83cfebc01480c4", "local_bytes": 52, "local_packets": 1, "other_bytes": 0, "other_packets": 0, "total_bytes": 52, "total_packets": 1}'
        # #durationdata = {'server_ip': '146.56.145.179', 'country': 'South Korea', 'date': '2022-10-23',
        #                 'local_ip': '10.0.0.5',
        #                 'start_time': '01:31:03.697491', 'end_time': '01:35:32.458024',
        #                 'host_server_name': 'minor-api-os.hoyoverse.com', 'other_ip': '47.242.34.135',
        #                 'duration': 268.7605, 'game': 'Genshin Impact', 'game_company': 'miHoYo', 'bytes': 34565,
        #                 'packets': 139}
        #durationdata = json.dumps(durationdata).encode('utf-8')
        paymentdata = b'{ "time": "2022-10-23 01:31:03.697491", "server_ip": "1.1.1.1","country": "South Korea", "local_ip": "10.0.0.1", "platform": "Android", "payment": "Google Play", "recentGame": "Genshin Impact", "host_name_server": "minor-api-os.hoyoverse.com"}'

        #data = raw(databyte)
        #data = duration(durationdata)
        data = payment(paymentdata)
        session.add(data)
        session.commit()
        time.sleep(1)
        print(paymentdata)
