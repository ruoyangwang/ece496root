build:
	javac -classpath lib/zookeeper-3.4.6.jar:lib/log4j-1.2.16.jar ZkConnector.java jobTracker.java handleClient.java Client.java Listener.java Worker.java
jobTracker:
	javac -Xlint:unchecked -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java JobTracker.java HandleClient.java JobObject.java
client:
	javac -Xlint:unchecked -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java Client.java
Worker:
	javac -Xlint:unchecked -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java Worker.java
run:
	java -classpath lib/zookeeper-3.4.6.jar:lib/log4j-1.2.16.jar:. jobTracker
clean:
	rm -f *.class
