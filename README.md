### Download
```bash
git clone https://github.com/zeroistfilm/Shell_Parser
```

### How to run the producer
```bash
sudo apt install python3-pip -y
sudo pip3 install aiokafka pytz

chmod +x json_capture.sh
chmod +x restart_producer.sh
./restart_producer.sh

# for cronjob
chmod +x run_producer.sh
crontab -e
3 0 * * * /home/ubuntu/Shell_Parser/run_producer.sh
3 12 * * * /home/ubuntu/Shell_Parser/run_producer.sh
```

### How to run the Kafka server
```bash
cd kafka
sudo docker-compose up -d
```

---
### How to run the consumer
build and push to DockerHub (need sign-in to henrychoi7 docker hub account)
```bash
docker build -t henrychoi7/gambit-consumer:latest . --no-cache
docker push henrychoi7/gambit-consumer:latest
```

run
```bash
sudo docker run -d henrychoi7/gambit-consumer:latest
#OR
cd consumer/
sudo docker-compose down
sudo docker-compose up -d

sudo docker ps -a
sudo docker stats
```
