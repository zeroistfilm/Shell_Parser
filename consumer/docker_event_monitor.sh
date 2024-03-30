#!/bin/bash

# Docker 이벤트를 모니터링하고, consumer-consumer-1 컨테이너가 종료되었을 때 이메일을 보내는 스크립트

docker events --filter 'event=die' --filter 'container=consumer-consumer-1' --format '{{.Status}}' |
while read event
do
    if [ "$event" == "die" ]; then
        # 이메일 보내는 명령어 (예: mail 또는 sendmail 등)를 여기에 추가
        echo "consumer-consumer-1 컨테이너가 종료되었습니다." | mail -s "컨테이너 종료 알림" handong@mareel.io
    fi
done

# nohup ./docker_event_monitor.sh > docker_event.log 2>&1 &