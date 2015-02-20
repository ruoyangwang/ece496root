#!/bin/bash
cnt=`ps aux|grep -v "grep" | grep "zoo" -c`
if [ "$cnt" -gt 0 ]
then
kill $(ps aux | grep -v "grep" | grep "zoo" | awk '{print $2}')
echo "Processes with name zoo killed"

else

echo "No process with name zoo found"
fi


cnt2=`ps aux|grep -v "grep"| grep "NPA" -c`
if [ "$cnt2" -gt 0 ]
then
kill $(ps aux | grep -v "grep" | grep "NPA" | awk '{print $2}')
echo "Processes with name NPA killed"

else

echo "No process with name NPA found"
fi



