#!/bin/bash

make clean
rm -f jobScheduler.db
cat test_schema.sql | sqlite3 jobScheduler.db
javac -Xlint:unchecked -classpath ../lib/sqlite-jdbc-3.8.7.jar:../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java JobObject.java WorkerObject.java Scheduler.java ScheduleAlgo.java testSch.java
java -classpath :../lib/sqlite-jdbc-3.8.7.jar testSch
echo "Clean up"
make clean
rm -f jobScheduler.db
