#!/bin/bash

#echo "Clean Cashied Memory"
#sudo ../../bin/clearcashe.sh

swapMemLimit=`grep "SwapCached"  /proc/meminfo | awk '{print int($2/1024)}'`
echo "Current Swapped Memory: "$swapMemLimit"MB"
#All NPAIRS jobs should not use more than 500MB swapped mem
swapMemLimit=500
maxJobs=0
PID=""
while true;
do
  echo ""
  echo "---------------"
  echo "Run NPAIRS with Q=50"
  echo "number of NPAIRS: "$((maxJobs + 1))
  ../../lib/jdk1.8.0_25/bin/java -Xmx8096m -jar ../../lib/npairs-20141008.jar NPAIRS RUN_ANALYSIS SetupFiles/RRSD_25Subjects_NPAIRSAnalysisSetup-num_splits\=1\,pcrange\=1-500.mat 50 &> /dev/null &
  PID=$!" "$PID

  sleep 60
  cashe=`grep "SwapCached"  /proc/meminfo | awk '{print int($2/1024)}'`
  echo "swap memory = "$cashe
  if [ $cashe -gt $swapMemLimit ];
  then
    break
  fi
  maxJobs=$((maxJobs + 1))
done

echo "kill all jobs"
kill $PID

echo ""
echo "Result max # of jobs: "$maxJobs
echo $maxJobs > ../../log/maxJobs.info
exit 0
