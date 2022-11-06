import datetime
import time
import random


class SignalTimer:
    def __init__(self, name, target, timeLimitSeconds, targetCount):
        self.name = name
        self.startTime = datetime.datetime.now()
        self.endTime = self.startTime + datetime.timedelta(seconds=timeLimitSeconds)
        self.currCount = 0
        self.target = target
        self.targetCount = targetCount
        self.candidateList = []
        self.targetList = []
        self.valve = False

    def getvalve(self):
        return self.valve

    def isEndWatch(self):
        return datetime.datetime.now() > self.endTime

    def add(self, data):
        self.candidateList.append(data)
        if data == self.target:
            self.currCount += 1
            self.targetList.append(data)

    def check(self):
        if self.targetCount <= self.currCount and self.isEndWatch():
            self.valve = True
        self.valve = False

    def __str__(self):
        return f"{self.name}, {self.check()}, {self.targetList}"


# 결제 시도 후 성공 실패 여부 확인
class paymentChecker:
    def __init__(self, local_ip):
        self.local_ip = local_ip

        self.urls = {"paly-fe.googleapis.com":              ("android", "ANDROID_PAYMENT_TRY_1"),
                     "play-fe.googleapis.com ":             ("android","ANDROID_PAYMENT_SUCCESS_1"),
                     "inbox.google.com":                    ("android","ANDROID_PAYMENT_SUCCESS_1_1"),
                     "play-lh.googleusercontent.com":       ("android","ANDROID_PAYMENT_FAIL"),

                     "p30-buy.itunes-apple.com.akadns.net": ("ios", "IOS_PAYMENT_TRY_1"),
                     "p30-buy-lb.itunes-apple.com.akadns.net": ("ios", "IOS_PAYMENT_TRY_2"),
                     "p30-buy.itunes.apple.com":                ("ios",  "IOS_PAYMENT_TRY_3"),
                     "xp.apple.com":                            ("ios", "IOS_PAYMENT_SUCCESS_1"),
                     "bag.itunes.apple.com":                    ("ios", "IOS_PAYMENT_SUCCESS_2"),
                     "pd.itunes.apple.com":                 ("ios", "IOS_PAYMENT_SUCCESS_3")}

        self.type = "android"
        if self.type == "android":
            self.tryGate = SignalTimer(name="gateTry", target="ANDROID_PAYMENT_TRY_1", timeLimitSeconds=1, targetCount=2)
            self.successGate1 = SignalTimer(name="gateSuccess1", target="ANDROID_PAYMENT_SUCCESS_1", timeLimitSeconds=1, targetCount=2)
            self.successGate2 = SignalTimer(name="gateSuccess1-1", target="ANDROID_PAYMENT_SUCCESS_1_1", timeLimitSeconds=5, targetCount=2)
            self.failGate1 = SignalTimer(name="gateFail1", target="ANDROID_PAYMENT_FAIL", timeLimitSeconds=1, targetCount=2)
            self.failGate2 = SignalTimer(name="gateFail2", target="ANDROID_PAYMENT_FAIL", timeLimitSeconds=20, targetCount=1)

        if self.type =="ios":
            self.tryGate1 = SignalTimer(name="gateTry", target="IOS_PAYMENT_TRY_1", timeLimitSeconds=2, targetCount=2)
            self.tryGate2 = SignalTimer(name="gateTry", target="IOS_PAYMENT_TRY_2", timeLimitSeconds=2, targetCount=1)
            self.tryGate3 = SignalTimer(name="gateTry", target="IOS_PAYMENT_TRY_3", timeLimitSeconds=2, targetCount=1)

            self.successGate1 = SignalTimer(name="gateSuccess1", target="ANDROID_PAYMENT_SUCCESS_1", timeLimitSeconds=30,targetCount=1)
            self.successGate2 = SignalTimer(name="gateSuccess1-1", target="ANDROID_PAYMENT_SUCCESS_2", timeLimitSeconds=30, targetCount=2)
            self.successGate3 = SignalTimer(name="gateSuccess1-1", target="ANDROID_PAYMENT_SUCCESS_3", timeLimitSeconds=30, targetCount=32)


    def check(self, data):
        if data.host_server_name not in self.urls:
            return False

        if self.tryGate.getvalve() == False:
            self.tryGate.add(self.urls[data.host_server_name])
            self.tryGate.check()

        elif self.tryGate.getvalve() == True:


            if self.successGate.getvalve() == False:
                self.successGate.add(self.urls[data.host_server_name])
                self.successGate.check()
            elif self.successGate.getvalve() == True:
                # 자료 저장
                print("success")


            if self.failGate.getvalve() == False:
                self.failGate.add(self.urls[data.host_server_name])
                self.failGate.check()
            elif self.failGate.getvalve() == True:
                # 자료 저장
                print("fail")


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



paymentchecker = paymentChecker("10.0.0.1")

