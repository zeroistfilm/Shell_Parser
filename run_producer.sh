#!/bin/bash

# Kill the previous processes (producer.py)
pids=$(ps -ef | grep "producer.py" | grep -v grep | awk '{print $2}')
for pid in $pids; do
    sudo kill -9 $pid
done

# Change directory (depends on system)
cd /home/ubuntu/Shell_Parser

# Remove nohup.out
sudo rm nohup.out

# Run the Python scripts with automatic Enter key presses
#nohup sudo python3 producer.py </dev/null >/dev/null 2>&1 &
#sudo python3 /home/ubuntu/Shell_Parser/producer.py </dev/null >/dev/null 2>&1 &
nohup sudo python3 producer.py </dev/null >./nohup.out 2>&1 &