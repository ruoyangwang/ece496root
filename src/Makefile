build:
	javac -classpath lib/zookeeper-3.4.6.jar:lib/log4j-1.2.16.jar ZkConnector.java jobTracker.java handleClient.java Client.java Listener.java
jobTracker:
	javac -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java JobTracker.java HandleClient.java
client:
	javac -classpath ../lib/zookeeper-3.4.6.jar:../lib/log4j-1.2.16.jar ZkConnector.java Client.java

clean:
	rm -f *.class
