#!/usr/bin/env bash

# 1 = host , 2 = zookeeper host 3 = input file

ssh $1 "cd ~/capstone/ece496root/bin && nohup ./worker.sh $2 >> ~/worker.$1.log &"
