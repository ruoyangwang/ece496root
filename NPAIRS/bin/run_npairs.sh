#!/bin/bash

#check input
minQ=1
maxQ=500
if [[ $1 != *_log ]];
then
  echo "Error: first param must be the log dir name end with _log, e.g. 1234_log"
  exit 1
fi

for arg in "${@:2}";
do
  if [[ "$arg" -lt "$minQ" || "$arg" -gt "$maxQ" ]]
  then
    echo "Error: Bad input Q values ->"$arg
    exit 1
  fi
done

#TODO: check NPARIS initialization

#Create log dir
log_dir=$1
echo "Creating log dir "$log_dir
rm -rf $log_dir
mkdir $log_dir

#start CPU/RAM monitor
echo "CPU/RAM usage log: "$log_dir"/cpu_mem_stat.cvs"
dstat -cm --output $log_dir/cpu_mem_stat.csv 30 &> /dev/null  &
dstat_pid=$!

pids=""

#run NPAIRS with Q values from args (skip first arg)
for qValue in ${@:2};
do
#  echo "Q= "$qValue
#  echo "Q= "$qValue > $log_dir/"NPAIRS_Q_"$qValue".log"
#  echo "All tasks completed in 324" > $log_dir/"NPAIRS_Q_"$qValue".log"
#  sleep 5 &
  ../../lib/jdk1.8.0_25/bin/java -Xmx8096m -jar ../../lib/npairs-20141008.jar NPAIRS RUN_ANALYSIS SetupFiles/RRSD_25Subjects_NPAIRSAnalysisSetup-num_splits\=1\,pcrange\=1-500.mat $qValue &> $log_dir/NPAIRS_Q_$qValue.log &
  pids=$pids" "$!
  sleep 1
done

#wait all NPAIRS
echo "pids= "$pids
wait $pids

kill $dstat_pid
echo "All NPAIRS runs are finished"

#Check whether all runs result
echo "Veify results"
for qValue in ${@:2};
do
  grep "All tasks completed in" $log_dir/NPAIRS_Q_$qValue.log &> /dev/null
  if [ "$?" -ne "0" ];
  then
    echo "Failed NPAIR: Q="$qValue
    echo $qValue >> $log_dir/failed_Q.log
  fi
done

#check if we have any failed runs, failed Q will be saved below
ls $log_dir"/failed_Q.log" &> /dev/null
if [ "$?" -eq 0 ];
then
  echo "exit with failed NPARIS run(s)"
  exit 2
fi

echo "Successful"
exit 0
