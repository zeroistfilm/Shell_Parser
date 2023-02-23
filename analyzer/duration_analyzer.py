import os

from sqlalchemy import create_engine, Column, Integer, String, Date, func, text
from mareeldatabase import mareelDB, createDurationTable
from datetime import datetime, timedelta
from collections import defaultdict
import copy
import json

mareeldb = mareelDB()
table = createDurationTable('Mareel_GO_Duration')
db = mareeldb.session

start_date = datetime(2022, 10, 25)
end_date = start_date + timedelta(days=7)
country = 'South Korea'
directory = "./analyzer/"+country.replace(' ','_')

#국가별필터링
rows = db.query(table).filter(table.country == country).all()
#전체
# rows = db.query(table).all()

# ======================================================일주일 간격으로 담기
start_date = datetime.strptime('2022-10-23', "%Y-%m-%d")
end_date = start_date + timedelta(7)
weeks = defaultdict(list)
i = 0
for row in rows:
    row_time = datetime.strptime(row.date + '_' + row.start_time, "%Y-%m-%d_%H:%M:%S.%f")
    if start_date < row_time and row_time < end_date:
        weeks[f'{start_date.strftime("%Y-%m-%d")}~{end_date.strftime("%Y-%m-%d")}'].append(row)

    elif end_date < row_time:
        start_date = end_date
        end_date = start_date + timedelta(7)
        weeks[f'{start_date.strftime("%Y-%m-%d")}~{end_date.strftime("%Y-%m-%d")}'].append(row)


for weekCount in weeks:
# 해당 주차에 플레이된 게임 리스트
    playedGame = set([x.game for x in weeks[weekCount]])

    # 게임별 기본 탬플릿
    gameTemplate = {
        'localIpCount': set(),
        'localIpFrequency': {},
        'localIpTotalDuration': 0,
        'localIpDuration': {}
    }

    gameDict = {}
    for game in playedGame:
        gameDict[game] = copy.deepcopy(gameTemplate)


    for row in weeks[weekCount]:
        ip = row.local_ip
        duration = float(row.duration)
        game = row.game

        gameDict[game]['localIpCount'].add(ip)

        if ip not in gameDict[game]['localIpFrequency']:
            gameDict[game]['localIpFrequency'][ip] = 1
        else:
            gameDict[game]['localIpFrequency'][ip] += 1

        gameDict[game]['localIpTotalDuration'] += duration

        if ip not in gameDict[game]['localIpDuration']:
            gameDict[game]['localIpDuration'][ip] = duration
        else:
            gameDict[game]['localIpDuration'][ip] += duration

    for game in playedGame:
        gameDict[game]['localIpCount'] = len(gameDict[game]['localIpCount'])
    print(f'"week{weekCount}" : ', gameDict)


    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(f'./{directory}/week{weekCount}.json', 'w') as f:
        # 파일에 쓰기
        json.dump(gameDict, f)


#=============================== 아래는 전체 데이터 구하는 코드
playedGame = set([x.game for x in rows])

# 게임별 기본 탬플릿
gameTemplate = {
    'localIpCount': set(),
    'localIpFrequency': {},
    'totalDuration': 0,
    'localIpDuration': {}
}

gameDict = {}
for game in playedGame:
    gameDict[game] = copy.deepcopy(gameTemplate)


for row in rows:
    ip = row.local_ip
    duration = float(row.duration)
    game = row.game

    gameDict[game]['localIpCount'].add(ip)

    if ip not in gameDict[game]['localIpFrequency']:
        gameDict[game]['localIpFrequency'][ip] = 1
    else:
        gameDict[game]['localIpFrequency'][ip] += 1

    gameDict[game]['totalDuration'] += duration

    if ip not in gameDict[game]['localIpDuration']:
        gameDict[game]['localIpDuration'][ip] = duration
    else:
        gameDict[game]['localIpDuration'][ip] += duration


for game in playedGame:
    gameDict[game]['localIpCount'] = len(gameDict[game]['localIpCount'])
print(gameDict)
with open(f'./{directory}/total.json', 'w') as f:
    # 파일에 쓰기
    json.dump(gameDict, f)