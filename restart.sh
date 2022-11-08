kill -9 $(ps -ef | grep producer | awk '{print $2}')
rm nohup.out
nohup python3 producer.py &