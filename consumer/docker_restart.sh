#!/bin/bash

# consumer-consumer-1 컨테이너를 모니터링하고, 종료되면 일정 시간 후 재시작하는 스크립트

# 모니터링할 컨테이너 이름
CONTAINER_NAME="consumer-consumer-1"

# 재시작까지의 대기 시간 (초)
WAIT_TIME=3

while true
do
    # 컨테이너의 상태 확인
    container_status=$(sudo docker ps -a --filter "name=${CONTAINER_NAME}" --format '{{.Status}}')

    # 컨테이너가 종료되었는지 확인
    if [[ "$container_status" == *"Exited"* ]]; then
        echo "컨테이너가 종료되었습니다. $WAIT_TIME 초 후에 재시작합니다."

        # 대기
        sleep $WAIT_TIME

        # 시스템 prune을 통해 사용하지 않는 컨테이너를 삭제
        sudo docker system prune -a -f

        # Docker Compose를 사용하여 컨테이너 재시작
        sudo docker compose -f /root/Shell_Parser/consumer/docker-compose.yml up -d

        # 이메일 보내기
        echo "컨테이너가 종료되어 재시작되었습니다." | mail -s "컨테이너 재시작 알림" handong@mareel.io

        # 스크립트 실행 종료
        exit
    fi

    # 10초마다 반복하여 상태 확인
    sleep 10
done
