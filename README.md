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
pip3 install pytz
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


---
### restart
```bash
chmod 777 restart_producer.sh
./restart_producer.sh
```

```bash
chmod 777 restart_consumer.sh
./restart_consumer.sh
```

