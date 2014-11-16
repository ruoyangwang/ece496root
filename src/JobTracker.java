import java.net.*;
import java.io.*;
import java.util.*;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Responsibilities of job tracker:
 * 1. Accept requests from client:
 *    a) new job request
 *    b) result request
 * 2. On new job request:
 *    a) Create an ID for the job
 *    b) Put job in jobpool 
 * 3. Reassign work of dead workers
 * 4. Create appropriate worker directories under /job
 * 5. Create appropriate directories in zookeeper if necessary
 * 6. Assign work to workers by requesting scheduler. 
 */

public class JobTracker {

    private static ZkConnector zkc;
	// Whether we are the primary or backup job tracker
	static int boss;

    // watcher for primary/backup of job tracker
	Watcher jobTrackerWatcher;
	// Watcher on workers directory
 	Watcher workerWatcher;

	static String jobTrackerServerInfo;

    public static ServerSocket serverSocket = null;

    final static String JOB_TRACKER_PATH = "/jobTracker";
	final static String WORKER_PATH = "/worker";
	final static String JOBS_PATH = "/jobs";
	final static String SEQ_PATH = "/seq";
	final static String RESULT_PATH = "/result";
	final static String JOBPOOL_PATH = "/jobpool";

	/** 
     * constructor for job tracker
     */
    public JobTracker(String hosts) {

 		System.out.println("constructing job tracker: " + hosts);
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper can not connect "+ e.getMessage());
        }
 		System.out.println("Zookeeper connected");
		// initialize watchers
        jobTrackerWatcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
								// Try to be 
								EventType type = event.getType();
								if (type == EventType.NodeDeleted) {
									System.out.println("jobTracker deleted! Let's go!");       
									tryToBeBoss();
								}
								/* This might not be needed.								
								if (type == EventType.NodeCreated) {
									System.out.println(myPath + " created!");       
									tryToBeBoss();
								}*/


                            } };
                            
        workerWatcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
								EventType type = event.getType();
								if (type == EventType.NodeChildrenChanged) {
									System.out.println("Workers Changed. Now handle it.");       
									balanceWorkerDir();
								}                         
                            } };
    }

	public static synchronized Integer getSequenceNum(){
    	Stat stat = null;
    	stat = zkc.exists(SEQ_PATH, null);
    	if(stat == null){
			System.out.println("Critical Error!! in getSequenceNum. " + SEQ_PATH + " does not exist");
    		return null;
    	}

		String seq = zkc.getData(SEQ_PATH, null, stat);
		
		if(seq != null){
			// handle the result
			Integer parsed;
            parsed = Integer.parseInt(seq);

			Integer newValue;
			newValue = new Integer(parsed.intValue()+1);

			Stat stats = zkc.setData(SEQ_PATH, newValue.toString(), -1);
			if(stats == null){
				System.out.println("Critical Error!! in getSequenceNum. " + SEQ_PATH + " can not set new value");
				return null;
			}
			return parsed;
		}else{
			System.out.println("Critical Error!! in getSequenceNum. can not get num");
			return null;
		}
	}


	/**
	 * Create a persistent foler with the specified path and value.
	 */
    private static synchronized void createOnePersistentFolder(String Path, String value){	
		// create folder
 		Stat stat = zkc.exists(Path,null);
        if (stat == null) { 
	        System.out.println("Creating " + Path);
	        Code ret = zkc.create(
	                    Path,         // Path of znode
	                    value,        // Data
	                    CreateMode.PERSISTENT   // Znode type, set to PERSISTENT.
	                    );
	        if (ret == Code.OK) {
				System.out.println(Path.toString()+" path created!");
	   	 	} else {
				System.out.println(Path.toString()+" path creation failed!");
			}
        }
    }

	/**
	 * Create persistent folders.
 	 */
    private void createPersistentFolders(){
		// create jobs folder
		createOnePersistentFolder(JOBS_PATH, null);

		// create seq folder
		createOnePersistentFolder(SEQ_PATH, "1");

		// create worker folder
		createOnePersistentFolder(WORKER_PATH, null);

		// create result folder
		createOnePersistentFolder(RESULT_PATH, null);

		// create jobpool folder
		createOnePersistentFolder(JOBPOOL_PATH, null);
    }

	/**
	 * Adds a job to the job pool
	 */
	public static synchronized void addJobIdToPool(String jobId) {
		Stat stat = zkc.exists(JOBPOOL_PATH, null);

		if (stat != null) {

			String addP = JOBPOOL_PATH + "/" + jobId;
		    System.out.println("Adding job id to jobpool: " + addP);

			createOnePersistentFolder(addP, null);

		} else {
			System.out.println(JOBPOOL_PATH + " does not exist in assignWork - 1");
		}
	}


	/**
	 * Adds a job to the job pool
	 */
	public static synchronized void addToJobPool(JobObject job) {
		Stat stat = zkc.exists(JOBPOOL_PATH, null);

		if (stat != null) {

			String addP = JOBPOOL_PATH + "/" + job.jobId.toString();
		    System.out.println("assigning job " + addP);
			Stat stat2 = zkc.exists(addP, null);

			if(stat2!=null){

				String addP2 = addP + "/" + job.nValue.toString();
				// add job back to the job pool
				createOnePersistentFolder(addP2, job.toJobString());

			} else {
				System.out.println(addP + " does not exist in addToJobPool - 2");
			}
		} else {
			System.out.println(JOBPOOL_PATH + " does not exist in addToJobPool - 1");
		}
	}

	/**
	 * Reassign work for a worker under job dir
	 */
	private void reassignWork(String removedWorker){
		// assigned job to the removed worker
		List<String> assignedList = zkc.getChildren(JOBS_PATH + "/" + removedWorker);
		
		Iterator l;
		l = assignedList.listIterator();
		Hashtable<String,String> newWork = new Hashtable<String,String>();

		while(l.hasNext()){
			// job-task name
			String jobTaskId = (String)l.next();
			Stat stat = null;

			String rPath = JOBS_PATH + "/" + removedWorker + "/" + jobTaskId;
			stat = zkc.exists(rPath, null); // see if node in jobList still exists in workerList
			if(stat == null){
				//error
				System.out.println("Critical Error!! in reassignWork. " + rPath + " does not exist");
			}else{

				// extract data

				String data = zkc.getData(rPath, null, stat);
		
				if(data != null){

					String [] temp = jobTaskId.split("-");
					String jobId = temp[0];
					String taskId = temp[1];

					System.out.println("Adding into jobpool jobTaskId:" + jobTaskId + " data:" + data);

					// task id is n value
					JobObject j = new JobObject(Integer.parseInt(jobId), Integer.parseInt(taskId));
					j.parseJobString(data);

					addToJobPool(j);

				}else{
					System.out.println("Critical Error!! in reassignWork. can not get data");
				}
				System.out.println("Deleting job: " + rPath);
				zkc.delete(rPath, -1);
			}
		}
	}



	/**
	 * Create dir for workers under /jobs if they newly joint
	 */
	private void handleJoinedWorkers (){
		// NOTE: Try the watch as second param.
		List<String> workerList = zkc.getChildren(WORKER_PATH); 

		ListIterator l;
		l=workerList.listIterator();
		
		while(l.hasNext()){
			String r = (String)l.next();
			Stat stat = null;
			String rPath = JOBS_PATH + r;
			stat = zkc.exists(rPath, null); // see if node in workerList exists in jobList

			if(stat == null){
				String wPath = JOBS_PATH + r;

				//create worker
				createOnePersistentFolder(wPath, null);

			}
		}
	}

	private void handleLeftWorkers() {
		List<String> jobList = zkc.getChildren(JOBS_PATH);

		ListIterator l;
		l=jobList.listIterator();
		while(l.hasNext()){
			String r = (String)l.next();
			Stat stat = null;
			String rPath = WORKER_PATH + "/" + r;
			stat = zkc.exists(rPath, null); // see if worker node still exists.
			if(stat == null){
				System.out.println("Worker left...");
				reassignWork(r);
				zkc.delete(rPath, -1);
			}
		}
	}

    private void balanceWorkerDir(){		
		System.out.println("See if new worker joined...");
		handleJoinedWorkers();

		System.out.println("See if any workers died...");
		handleLeftWorkers();
	
		//re-enable watch   
		List<String> stats = zkc.getChildren(WORKER_PATH, workerWatcher);

		if(stats == null){		
			System.out.println("ERR: " + WORKER_PATH + " does not exist ");
		}

		//System.out.println("workerWatcher set - 2");
    }


	/**
	 * Try to be the boss by checking and trying to create the jobTracker dir
     * Check if the necessary paths are already created.
     */
    private void tryToBeBoss() {
        Stat stat = zkc.exists(JOB_TRACKER_PATH, jobTrackerWatcher);
		
		// JOB_TRACKER_PATH doesn't exist; let's try creating it
        if (stat == null) {              
            System.out.println("Creating " + JOB_TRACKER_PATH);
            Code ret = zkc.create(
                        JOB_TRACKER_PATH,       // Path
                        jobTrackerServerInfo,   // information
                        CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
                        );

            if (ret == Code.OK) {
				// If we successfully created the jobTracker folder, we are the boss.
				boss =1;

				System.out.println("Primary jobTracker!");
				createPersistentFolders();

				balanceWorkerDir();
			}
        } 
    }


	// Main function for job tracker
	// arg[0] is the port of zookeeper
	// arg[1] is the port for job tracker to listen to.
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. jobTracker zkServer:zkPort jobTrackerPort");
            return;
        }
		
		// Assume we are backup until we fight for it.
		boss =0;

		try{
			// create a server socket to listen on
        	serverSocket = new ServerSocket(Integer.parseInt(args[1]));
		}catch (Exception e){
			System.out.println("Failed to create server socker");
			return;
		}		
		
		String zookeeperLocation = args[0];
		String portToListen = args[1];
		String myHostName;

		try{
			myHostName = InetAddress.getLocalHost().getHostName();
		}catch (Exception e){
			System.out.println("Failed to get host name");
			return;
		}		

		jobTrackerServerInfo = myHostName + ":" + portToListen;
        System.out.println("Location of jobTracker: "+jobTrackerServerInfo);

        JobTracker t = new JobTracker(zookeeperLocation);   
		
        System.out.println("Sleeping...");
        t.tryToBeBoss();
        while (boss==0) {
            try{ Thread.sleep(1000); } catch (Exception e) {}
        }

        while (boss==1) {
				System.out.println("Listening...");
			try{Thread.sleep(5000);
        		new HandleClient(serverSocket.accept()).start();     
			}catch (Exception e){
				System.out.println("Failed to create new HandleClient");
			}		   	
        }

		
        //serverSocket.close();
		
    }

}
