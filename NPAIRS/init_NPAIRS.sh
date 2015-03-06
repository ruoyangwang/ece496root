#!/bin/bash

#This script will setup NPAIRS and calculate max # of NPARS can be run parall

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

#Specify input image data file name without exetention
#DATA_FILE="fmri_Resting-State-NPAIRS"
DATA_FILE="$1"

bin/kill_all_npair.sh

#unpack jdk
echo "Unpack jdk"
echo ""
rm -rf lib/jdk1.8.0_25
tar zxvf lib/jdk-8u25-linux-x64.tar.gz -C lib &> /dev/null
if [ $? -ne 0 ];then
  echo "tar jdk failed"
  exit 1
fi

#unpack data
echo "Unpack fmri data"
echo ""
rm -rf data/$DATA_FILE
tar zxvf data/$DATA_FILE.tar.gz -C data &> /dev/null
if [ $? -ne 0 ];then
  echo "tar fmri data failed"
  exit 1
fi

mkdir log &> /dev/null

echo "Initialize NPAIRS packege setup (without Q value)"
echo ""
cd data/$DATA_FILE
../../lib/jdk1.8.0_25/bin/java -Xmx8096m -jar ../../lib/npairs-20141008.jar NPAIRS RUN_ANALYSIS SetupFiles/RRSD_25Subjects_NPAIRSAnalysisSetup-num_splits\=1\,pcrange\=1-500.mat &> $DIR/log/init.log &
PID=$!
cd -

echo "Wating ..."
while true;
do
  grep "Data loading/EVD completed in" log/init.log &> /dev/null
  if [ $? -eq 0 ];then
    break
  fi
  sleep 30
  echo "Not done, check in 30 secs"
done

echo "fmri data is initialized"
kill $PID

#Creating log file
echo "1" > log/init_done.info

echo "Create script soft links"
echo "Free free to terminate ctr-c"
cd data/$DATA_FILE
ln -s ../../bin/* .

#echo "Ran NPAIRS multiProc benchmark"
#./npairs_multiProc_benchmark.sh
#wait
#cd -

exit 0
