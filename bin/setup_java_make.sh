#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

echo "untar java"
cd ../NPAIRS/
#unpack jdk
ls ~/jdk1.8.0_25 &> /dev/null
if [ $? -ne 0 ];then
  echo "Untar jdk"
  tar zxvf lib/jdk-8u25-linux-x64.tar.gz -C ~/ &> /dev/null
fi

source $DIR/NPAIRS.env

echo "Make src"
cd $DIR/../src
make
