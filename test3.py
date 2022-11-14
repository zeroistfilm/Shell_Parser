import datetime


class SignalTimer:
    def __init__(self, name, target, timeLimitSeconds, targetCount):
        self.name = name
        self.timeLimitSeconds = timeLimitSeconds
        self.startTime = None
        self.endTime = None
        self.currCount = 0
        self.target = target
        self.targetCount = targetCount
        self.candidateList = []
        self.targetResultList = []
        self.valve = False

    def isStart(self):
        return self.startTime is not None

    def setStart(self):
        self.startTime = datetime.datetime.now()
        self.endTime = self.startTime + datetime.timedelta(seconds=self.timeLimitSeconds)

    def getvalve(self):
        return self.valve

    def isEndWatch(self):
        return datetime.datetime.now() > self.endTime

    def add(self, data):
        self.candidateList.append(data)
        if data == self.target:
            self.currCount += 1
            self.targetResultList.append(data)
        self.check()

    def check(self):
        if self.targetCount <= self.currCount and not self.isEndWatch():
            self.valve = True
            return True
        self.valve = False
        return False


# 결제 시도 후 성공 실패 여부 확인
class PaymentChecker:
    def __init__(self, local_ip):
        self.local_ip = local_ip

        self.urls = {"None-play-fe.googleapis.com":
                         {'type': "android", 'title': "ANDROID_PAYMENT_TRY", 'timeLimit': 1, 'targetCount': 2},
                     "Trying-play-fe.googleapis.com":
                         {'type': "android", 'title': "ANDROID_PAYMENT_SUCCESS", 'timeLimit': 1, 'targetCount': 2},
                     "Success_step1-inbox.google.com":
                         {'type': "android", 'title': "ANDROID_PAYMENT_SUCCESS", 'timeLimit': 5, 'targetCount': 1},
                     "Trying-play-lh.googleusercontent.com":
                         {'type': "android", 'title': "ANDROID_PAYMENT_FAIL", 'timeLimit': 20, 'targetCount': 1},

                     "None-p30-buy.itunes-apple.com.akadns.net":
                         {'type': "ios", 'title': "IOS_PAYMENT_TRY", 'timeLimit': 2, 'targetCount': 2},
                     "None-p30-buy-lb.itunes-apple.com.akadns.net":
                         {'type': "ios", 'title': "IOS_PAYMENT_TRY", 'timeLimit': 2, 'targetCount': 1},
                     "None-p30-buy.itunes.apple.com":
                         {'type': "ios", 'title': "IOS_PAYMENT_TRY", 'timeLimit': 2, 'targetCount': 1},
                     "Trying-xp.apple.com":
                         {'type': "ios", 'title': "IOS_PAYMENT_SUCCESS", 'timeLimit': 30, 'targetCount': 1},
                     "Trying-bag.itunes.apple.com":
                         {'type': "ios", 'title': "IOS_PAYMENT_SUCCESS", 'timeLimit': 30, 'targetCount': 2},
                     "Trying-pd.itunes.apple.com":
                         {'type': "ios", 'title': "IOS_PAYMENT_SUCCESS", 'timeLimit': 30, 'targetCount': 3}, }

        self.status = 'None'
        self.signalTimers = {}

    def getSignalTimer(self, key):
        if self.status not in self.signalTimers:
            self.signalTimers[self.status] = SignalTimer(name=self.urls[key]['title'], target=key,
                                                         timeLimitSeconds=self.urls[key]['timeLimit'],
                                                         targetCount=self.urls[key]['targetCount'])
        st = self.signalTimers[self.status]
        return st

    def changeStatus(self, key, status):
        st = self.getSignalTimer(key)
        if st.valve is False:
            if st.isStart() is False:
                st.setStart()
            st.add(key)
            if st.valve is True:
                del self.signalTimers[self.status]
                self.status = status
            return None

    def pipe(self, data):
        host_server_name = data['host_server_name']
        key = self.status + '-' + host_server_name

        if key not in self.urls:
            return None

        if self.urls[key]['type'] == 'android':
            if self.status == 'None' and self.urls[key]['title'] == 'ANDROID_PAYMENT_TRY':
                self.changeStatus(key, 'Trying')
                return None

            if self.status == 'Trying' and self.urls[key]['title'] == 'ANDROID_PAYMENT_SUCCESS':
                self.changeStatus(key, 'Success_step1')
                return None

            if self.status == 'Success_step1' and self.urls[key]['title'] == 'ANDROID_PAYMENT_SUCCESS':
                self.changeStatus(key, 'Success')
                return None

            if self.status == 'Trying' and self.urls[key]['title'] == 'ANDROID_PAYMENT_FAIL':
                self.changeStatus(key, 'Fail')
                return None

        if self.urls[key]['type'] == 'ios':
            if self.status == 'None' and self.urls[key]['title'] == 'IOS_PAYMENT_TRY':
                self.changeStatus(key, 'Trying')
                return None
            if self.status == 'Trying' and self.urls[key]['title'] == 'IOS_PAYMENT_SUCCESS':
                self.changeStatus(key, 'Success')
                return None



targetList = ["play-fe.googleapis.com",
              "play-fe.googleapis.com ",
              "inbox.google.com",
              "play-lh.googleusercontent.com",
              "play-lh.googleusercontent.com",
              "p30-buy.itunes-apple.com.akadns.net",
              "p30-buy-lb.itunes-apple.com.akadns.net",
              "p30-buy.itunes.apple.com",
              "xp.apple.com",
              "bag.itunes.apple.com",
              "pd.itunes.apple.com"]

import time
# #안드로이드 성공
# paymentchecker = PaymentChecker("10.0.0.1")
# print(paymentchecker.status)
# paymentchecker.pipe({'host_server_name': 'play-fe.googleapis.com'})
# paymentchecker.pipe({'host_server_name': 'play-fe.googleapis.com'})
#
# print(paymentchecker.status)
#
# time.sleep(3)
# paymentchecker.pipe({'host_server_name': 'play-lh.googleusercontent.com'})
# print(paymentchecker.status)
#
#
# #안드로이드 실패
# paymentchecker = PaymentChecker("10.0.0.1")
# print(paymentchecker.status)
# paymentchecker.pipe({'host_server_name': 'play-fe.googleapis.com'})
# paymentchecker.pipe({'host_server_name': 'play-fe.googleapis.com'})
#
# print(paymentchecker.status)
# time.sleep(0.5)
#
# #성공케이스
# paymentchecker.pipe({'host_server_name': 'play-fe.googleapis.com'})
# paymentchecker.pipe({'host_server_name': 'play-fe.googleapis.com'})
# print(paymentchecker.status)
# time.sleep(3)
# paymentchecker.pipe({'host_server_name': 'inbox.google.com'})
# print(paymentchecker.status)

# #ios 성공
# paymentchecker = PaymentChecker("10.0.0.1")
# print(paymentchecker.status)
# paymentchecker.pipe({'host_server_name': 'p30-buy.itunes-apple.com.akadns.net'})
# time.sleep(1)
# paymentchecker.pipe({'host_server_name': 'p30-buy.itunes-apple.com.akadns.net'})
# time.sleep(3)
# paymentchecker.pipe({'host_server_name': 'xp.apple.com'})
# time.sleep(5)
#
# print(paymentchecker.status)
#
# #ios 성공
# paymentchecker = PaymentChecker("10.0.0.1")
# print(paymentchecker.status)
# paymentchecker.pipe({'host_server_name': 'p30-buy-lb.itunes-apple.com.akadns.net'})
# time.sleep(0.5)
# paymentchecker.pipe({'host_server_name': 'p30-buy-lb.itunes-apple.com.akadns.net'})
#
#
# time.sleep(3)
# paymentchecker.pipe({'host_server_name': 'bag.itunes.apple.com'})
# paymentchecker.pipe({'host_server_name': 'bag.itunes.apple.com'})
# paymentchecker.pipe({'host_server_name': 'bag.itunes.apple.com'})
# time.sleep(5)
# print(paymentchecker.status)


#ios 성공
paymentchecker = PaymentChecker("10.0.0.1")
print(paymentchecker.status)
paymentchecker.pipe({'host_server_name': 'p30-buy.itunes.apple.com'})
time.sleep(0.5)
paymentchecker.pipe({'host_server_name': 'p30-buy.itunes.apple.com'})

time.sleep(3)
paymentchecker.pipe({'host_server_name': 'pd.itunes.apple.com'})
time.sleep(3)
paymentchecker.pipe({'host_server_name': 'pd.itunes.apple.com'})
time.sleep(3)
paymentchecker.pipe({'host_server_name': 'pd.itunes.apple.com'})

time.sleep(5)
print(paymentchecker.status)