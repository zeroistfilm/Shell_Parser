sudo kill -9 $(ps -ef | grep producer | awk '{print $2}')
sudo rm nohup.out
nohup sudo python3 producer.py &


