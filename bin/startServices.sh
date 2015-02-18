nohup ./zkServer.sh start-foreground > ~/zookeeper.log& nohup ./jobTracker.sh $1 8002 > ~/jobtracker.log& nohup ./scheduler.sh > ~/scheduler.log&

