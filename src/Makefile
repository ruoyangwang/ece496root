all: jobTracker client Worker scheduler

jobTracker:
	javac -Xlint:unchecked -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java JobTracker.java  WorkerObject.java HandleClient.java JobObject.java JobStatusObject.java
client:
	javac -Xlint:unchecked -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java Client.java

Worker:
	javac -Xlint:unchecked -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java Worker.java WorkerObject.java JobObject.java WorkerThreadHandler.java ResultObject.java JobStatusObject.java
scheduler:
	javac -Xlint:unchecked -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java JobObject.java WorkerObject.java Scheduler.java ScheduleAlgo.java
clean:
	rm -f *.class *.*~
