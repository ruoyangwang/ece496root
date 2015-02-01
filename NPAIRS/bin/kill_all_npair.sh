#/bin/bash

usrname=`whoami`
for i in `ps aux | grep $usrname | grep "NPAIRS RUN_ANALYSIS" | awk '{print $2}'`;
do
    kill $i
done
