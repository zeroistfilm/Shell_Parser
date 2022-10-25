### how to run the project  
```bash
chmod 777 json_capture.sh
```

```bash
python3 main.py
```

### background running  
```bash
nohup python3 main.py &
```

---
### log를 생성하는 서버에 producer 실행
init
```bash
apt install python3-pip -y
pip3 install aiokafka
그 외 기타 다른 필요한 라이브러리 설치 서버마다 상황 다름
```

run
```bash
nohup python3 producer.py &
```



### log를 수집하는 서버에서 consumer 실행
init
```bash 
apt install python3-pip -y
pip3 install aiokafka
pip3 install mysql-python

```
run
```bash
nohup python3 consumer.py &
```