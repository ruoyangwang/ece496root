ece496root
==========

scripts under /bin:

1. start zookeeper service:
	./startServices.sh ZookeeperNodeHostname (i.e.: ./startServices.sh c123)

2. start client user interface then follow the printed instructions:
	./client.sh 

3. whenever want to kill services and process:
	./killService.sh

4. whenever want to remove persistent data on Zookeeper:
	./clearEverything.sh


execution profiles:

/NPAIRS/log/maxJobs.info

change the number to tell worker how many jobs can be execute in parallel for that node


All runtime logs will be generated under root directory
