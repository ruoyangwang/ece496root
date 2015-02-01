#!/bin/bash

################################
##usage: ./run_npairs.sh $Q_Value
##exit code:
##0: NPAIRS run successful
##1: Bad arg
##2: NPAIR run failed
#################################

#check input
ls SessionFiles/ &> /dev/null
if [ $? -ne 0 ];
then
  echo "Please run under data/\$DATA_NAME/ dir"
  exit 1
fi

#Check args
minQ=1
maxQ=500
if [ "$#" -ne 1 ];
then
  echo "Error: Invalid # of arg"
  echo "Usage: First arg must be a Q value"
  exit 1
fi

if [[ "$1" -lt "$minQ" || "$1" -gt "$maxQ" ]]
then
  echo "Error: Bad input Q values range ->"$1
  echo "Usage: Q value range from 1 to 500(inclusive)"
  exit 1
fi

#Create log dir
DATA_NAME=$(basename $(pwd))
log_dir="../../log/$DATA_NAME"
echo "Creating log dir "$log_dir
mkdir -p $log_dir

#start CPU/RAM monitor
USAGE_LOG="log_dir/cpu_mem_stat_Q_$1.cvs"
echo "CPU/RAM usage log: $USAGE_LOG"
dstat -cm --output $USAGE_LOG 30 &> /dev/null  &
dstat_pid=$!


#run NPAIRS with Q values 
echo "Run NPAIRS with Q="$1
echo "Expect LONG waiting time"
NPAIRS_RUN_LOG="$log_dir/NPAIRS_RUN_Q_$1.log"
echo "Log file: "$NPAIRS_RUN_LOG

../../lib/jdk1.8.0_25/bin/java -Xmx8096m -jar ../../lib/npairs-20141008.jar NPAIRS RUN_ANALYSIS SetupFiles/RRSD_25Subjects_NPAIRSAnalysisSetup-num_splits\=1\,pcrange\=1-500.mat $1 &> $NPAIRS_RUN_LOG &
pid=$!
sleep 1

#wait all NPAIRS
echo "pid= "$pid
wait $pid

kill $dstat_pid
echo "NPAIRS run is finished"

#Check NPAIRS result
echo "Veify result"
grep "All tasks completed in" $NPAIRS_RUN_LOG &> /dev/null
if [ "$?" -ne "0" ];
then
  echo "Failed NPAIR: Q="$qValue
  exit 2
fi

echo "Successful"
exit 0
