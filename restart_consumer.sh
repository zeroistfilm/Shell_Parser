kill -9 $(ps -ef | grep consumer | awk '{print $2}')
#rm Mareel_GO_log.out Mareel_PRO_log.out Mareel_VPN_log.out

nohup python3 consumer_GO.py &> Mareel_GO_log.out &
nohup python3 consumer_PRO.py &> Mareel_PRO_log.out &
nohup python3 consumer_VPN.py &> Mareel_VPN_log.out &
#nohup python3 consumer_GO.py 1> /dev/null 2>&1 &
#nohup python3 consumer_PRO.py 1> /dev/null 2>&1 &
#nohup python3 consumer_VPN.py 1> /dev/null 2>&1 &
