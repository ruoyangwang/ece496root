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
 * 1. Accept requests from client and create a HandleCliet thread for the request.
 * 2. On new job request:
 *    a) Create an ID for the job
 *    b) Put job in jobpool 
 * 3. Track job results.
 * 4. Reassign work of dead workers
 * 5. Create appropriate worker directories under /jobs
 * 6. Create appropriate directories in zookeeper if necessary
 *
 * @author Jie (Jacky) Liu, jjiiee.liu@mail.utoronto.ca
 */

public class JobTracker {

    private static ZkConnector zkc;

	// Whether we are the primary or backup jobtracker
	static int boss;

    // Watcher for primary/backup of jobtracker
	Watcher jobTrackerWatcher;
	// Watcher on workers directory
 	Watcher workerWatcher;

	// Host and port number of the jobtracker
	static String jobTrackerServerInfo;

    public static ServerSocket serverSocket = null;

	// Input file and job id of the job currently being executed.
	// This is needed when a worker needs to be added to the pool.
	public static String CurrentJobFile = null;
	public static String CurrentJobId = null;

    final static String JOB_TRACKER_PATH = "/jobTracker";
	final static String WORKER_PATH = "/worker";
	final static String JOBS_PATH = "/jobs";
	final static String SEQ_PATH = "/seq";
	final static String RESULT_PATH = "/result";
	final static String JOBPOOL_PATH = "/jobpool";
	final static String CURRENT_JOB_PATH = "/currentJob";

	final static boolean DEBUG = false;

	// host and port number for the zookeeper
	static String ZookeeperLocation = null;

	/** 
     * Constructor for jobtracker 
	 * Connect to zookeeper via ZkConnector and initialize watchers.
	 *
	 * @param zooLocation - hostname and port number of zookeeper to connect to
     */
    public JobTracker(String zooLocation) {
		if (DEBUG)
	 		System.out.println("constructing job tracker: " + zooLocation);
    
	    zkc = new ZkConnector();
        try {
            zkc.connect(zooLocation);
        } catch(Exception e) {
            System.out.println("Zookeeper can not connect "+ e.getMessage());
        }
		if (DEBUG)
	 		System.out.println("Zookeeper connected");

		// initialize watchers
		// watch to become the primary job tracker
        jobTrackerWatcher = new Watcher() { // Anonymous Watcher
		    @Override
		    public void process(WatchedEvent event) {
				if (DEBUG)
					System.out.println("--- In jobTrackerWatcher ---");

				// Try to be the primary jobtracker
				EventType type = event.getType();
				if (type == EventType.NodeDeleted) {
					if (DEBUG)
						System.out.println("jobTracker deleted! Let's go!");       
					tryToBeBoss();
				}
	        } 
		};
        // watch for worker changes
        workerWatcher = new Watcher() { // Anonymous Watcher
		    @Override
		    public void process(WatchedEvent event) {
				if (DEBUG)
					System.out.println("--- In workerWatcher ---");
				EventType type = event.getType();
				// If the workers under /worker changed, /jobs needs to be updated.
				if (type == EventType.NodeChildrenChanged) {
					if (DEBUG)
						System.out.println("Workers Changed. Now handle it.");       
					balanceWorkerDir();
				}                         
		    } 
		};
    }

	/**
	 * Get zookeeper location.
	 * Returns the host of zookeeper. This is used 
	 * by HandleClient.java to start workers.
	 *
	 * @return - zookeeper hostname, null if unavailable.
	 */
	public static String getZookeeperHost () {
		if (ZookeeperLocation == null)
			return null;
		String[] location = ZookeeperLocation.split(":");
		return location[0];
	}

	/**
	 * Get the next sequence number.
	 * Get the next sequence number by increamenting 
	 * value in /seq
	 *
	 * @return - sequence number, null if unavailable.
	 */
	public static synchronized Integer getSequenceNum(){
    	Stat stat = null;
    	stat = zkc.exists(SEQ_PATH, null);
    	if(stat == null){
			System.out.println("Critical Error!! in getSequenceNum. " + SEQ_PATH + " does not exist");
    		return null;
    	}

		String seq = zkc.getData(SEQ_PATH, null, stat);
		
		if(seq != null){
			Integer curSeqNum = Integer.parseInt(seq);
			Integer newValue = new Integer(curSeqNum.intValue()+1);

			Stat stats = zkc.setData(SEQ_PATH, newValue.toString(), -1);
			if(stats == null){
				System.out.println("Critical Error!! in getSequenceNum. " + SEQ_PATH + " can not set new value");
				return null;
			}
			return curSeqNum;
		}else{
			System.out.println("Critical Error!! in getSequenceNum. can not get num");
			return null;
		}
	}

	/**
	 * Determine if workers are already started or not.
	 * Given a list of hostnames, determine if workers have been
	 * started on those hosts. Those that are already started are
	 * removed from the original list passed in. A list of started
	 * hosts is returned.
	 *
	 * NOTE: Since workers register with zookeeper after they finished
	 * initialization. Those that have not finished initialization will
	 * be considered "not started".
	 * @return - a list of started workers
	 */
	public static synchronized List<String> workersNotStarted(List hosts) {
		List<String> workerList = zkc.getChildren(WORKER_PATH); 
		List<String> started = new ArrayList<String>();
		Iterator l=workerList.listIterator();
		Stat s = null;
		while(l.hasNext()){
			String r = (String)l.next();
			String rPath = WORKER_PATH + "/" + r;
			String data = zkc.getData(rPath, null, s); // see if node in workerList exists in jobList

			if(data != null) {
				WorkerObject wo = new WorkerObject();
				wo.parseNodeString(data);
				String hostName = wo.getHostName();
				if (hostName != null) {
					if (hosts.contains(hostName)) {
						hosts.remove(hostName);
						started.add(hostName);
					}
				}
			}
		}
		return started;
	}

	/**
	 * Create a persistent znode(folder) with the specified path and value.
	 *
	 * @param path - path of the znode to be created
	 * @param value - value to be stored in the znode
	 */
    private static synchronized void createOnePersistentFolder(String path, String value){	
 		Stat stat = zkc.exists(path,null);
        if (stat == null) { 
	        System.out.println("Creating " + path);
	        Code ret = zkc.create(
	                    path,         // Path of znode
	                    value,        // Data
	                    CreateMode.PERSISTENT   // Znode type, set to PERSISTENT.
	                    );			
	        if (ret == Code.OK) {
				if (DEBUG)
					System.out.println(path.toString()+" path created!");
	   	 	} else {
				System.out.println(path.toString()+" path creation failed!");
			}
        } else {
			System.out.println(path.toString() + " path already exists");
		}
    }

	/**
	 * Set the current job being run by workers. 
	 *
	 * @param filename - the filename being run.
	 * @param jobId - the job id of the current job.
	 */
	public static synchronized void setCurrentJob (String filename, String jobId) {
		CurrentJobFile = filename;
		CurrentJobId = jobId;

		zkc.setData(CURRENT_JOB_PATH, filename + ":" + jobId, -1);
 	}

	/**
	 * Get the job currently being executed from zookeeper.
	 */
	private static void getCurrentJob() {
		Stat stat = null;
		String data = zkc.getData(CURRENT_JOB_PATH, null, stat);
		if (data != null && data.length() > 1) {
			String[] info = data.split(":");
			CurrentJobFile = info[0];
			CurrentJobId = info[1];
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
		createOnePersistentFolder(JOBPOOL_PATH, "1");

		// create current job folder
		createOnePersistentFolder(CURRENT_JOB_PATH, null);
    }

	/**
	 * Kill a job by setting the status in /result/{jobId} to 'killed'
	 * and remove all jobs in jobpool under the job id. Modifying the 
	 * status would trigger workers to leave. 
	 * 
	 * @param jobId - id of the job to be killed.
	 */
	public static void killJob (String jobId) {		
		// set jobto killed in result path this will trigger workers to exit.
		if (jobId == null || jobId.length() == 0) {
			return;
		}
		String p = RESULT_PATH + "/" + jobId;
		Stat stat = zkc.exists(p, null);
		if (stat != null) {
			int totalJobs = -1;
			String data = zkc.getData(p, null, stat);
			JobStatusObject jso = new JobStatusObject();
			jso.toObj(data);
			// set status to killed.
			jso.killed();		
			zkc.setData(p, jso.toZnode(), -1);
		}

		// remove jobs in jobpool.
		String jp = JOBPOOL_PATH + "/" + jobId;
	    stat = zkc.exists(jp, null);
		if (stat != null) {
			List<String> children = zkc.getChildren(jp);
			for (String jobName: children) {
				zkc.delete(jp + "/" + jobName, -1);
			}
			// signal work reassigned for scheduler to empty its job list.
			signalWorkReassigned();
		}
	}
	
	/**
	 * Return the job that is currently executing (ie: not killed or completed).
	 * @return - job id of the job currently executing, null if no jobs are running.
	 */
	public static String getRunningJob() {
		Stat stat = zkc.exists(JOBPOOL_PATH, null);
		String unfinished = null;
		if (stat != null) {
			List<String> allJobs = zkc.getChildren(JOBPOOL_PATH);
			for(String j : allJobs) {
				int re = checkStatus(j);
				if (re == 0){
					unfinished = j;
					if (DEBUG)
						System.out.println("Job ID: " + j + " incomplete");
					break;
				}
			}
		}

		return unfinished;
	}

	/**
	 * Get the job status object if the job is completed.
	 *
	 * @param jobId - id of the job 
	 * @return - JobStatusObject of the completed job, null if the job is not completed.
	 */
	public static JobStatusObject getCompletedJobStatus(String jobId) {
		int completed = checkStatus(jobId);
		if (completed != 1) {
			return null;
		} else {
			Stat stat = null;
			String data = zkc.getData(RESULT_PATH + "/" + jobId, null, stat);
			JobStatusObject jso = new JobStatusObject();
			jso.toObj(data);
			return jso;
		}
    }

	/**
	 * Check the status of job specified.
	 * @param jobId - id of the job
	 * @return - 1 if the job is completed, 2 if the job is killed, 0 if the job is unfinished, -1 on error
	 */
	public static int checkStatus(String jobId) {
		String p = RESULT_PATH + "/" + jobId;
		Stat stat = zkc.exists(p, null);
		if (stat != null) {
			int totalJobs = -1;
			String data = zkc.getData(p, null, stat);
			if (data != null) {
				JobStatusObject jso = new JobStatusObject();
				jso.toObj(data);

				if (jso.status.equalsIgnoreCase(new String("KILLED"))) {
					// killed
					return 2;
				} else if (jso.status.equalsIgnoreCase(new String("COMPLETED"))) {
					// finished
					return 1;
				}
				totalJobs = jso.totalJobs;
				
				if (totalJobs < 0) {
					System.out.println("ERROR: path: " + p + " has invalid data");
				} else {
					int finished = zkc.getChildren(p).size();
					
					if (finished == totalJobs) {
						jso.completed();
						zkc.setData(p, jso.toZnode(), -1);
						
						// finished
						return 1;
					} else {
						// unfinished
						return 0;
					}
				}
			} else {
				System.out.println("ERROR: path: " + p + " has no data");
			}
		} else {
			System.out.println("ERROR: path: " + p + " does not exist");
		}
		return -1;
	}

	/**
	 * Adds a jobid to jobpool and result paths
	 * Creates the znode /jobpool/{jobId} for jobs to be added. 
	 * Also creates the znode /result/{jobId} to track execution 
	 * time and status of the job.
	 *
	 * @param jobId - the job id to be added 
	 * @param count - the number of jobs (Q value analysis) for this job id.
	 */
	public static synchronized void addJobIdToPool(String jobId, Integer count) {
		if (jobId == null || jobId.length() == 0 || count == null) {
			return;
		}
		// Creating /jobpool/{jobId}
		Stat stat = zkc.exists(JOBPOOL_PATH, null);
		if (stat != null) {

			String addPath = JOBPOOL_PATH + "/" + jobId;
			if (DEBUG)
			    System.out.println("Adding job id to jobpool: " + addPath);

			createOnePersistentFolder(addPath, null);

		} else {
			System.out.println(JOBPOOL_PATH + " does not exist in addJobIdToPool()");
		}

		// Creating /result/{jobId}
		stat = zkc.exists(RESULT_PATH, null);
		if (stat != null) {
			String addPath = RESULT_PATH + "/" + jobId;
			JobStatusObject jso = new JobStatusObject(count);
			// record the start time of this job.
			jso.startTime = System.currentTimeMillis();

		    System.out.println("Adding job id to result path: " + addPath + "with data: "+jso.toZnode());
			createOnePersistentFolder(addPath, jso.toZnode());

		} else {
			System.out.println(RESULT_PATH + " does not exist in addJobIdToPool()");
		}
	}


	/**
	 * Adds a job to the jobpool under the appropriate job id.
	 * @param job - JobObject of the job to be aded.
	 * @return - ture if the job was successfully added, false otherwise.
	 */
	public static synchronized boolean addToJobPool(JobObject job) {
		Stat stat = zkc.exists(JOBPOOL_PATH, null);

		if (stat != null) {
			// check if job id was killed before adding to jobpool
			String p = RESULT_PATH + "/" + job.jobId.toString();
			stat = zkc.exists(p, null);
			if (stat != null) {
				int totalJobs = -1;
				String data = zkc.getData(p, null, stat);
				JobStatusObject jso = new JobStatusObject();
				jso.toObj(data);

				if (jso.status.equalsIgnoreCase(new String("KILLED"))) {
					return false;
				}
			} else {
				System.out.println(p + " does not exist in addToJobPool()");
				return false;
			}

			String addPath = job.getJobpoolParentPath();
			if (DEBUG)
			    System.out.println("assigning job " + addPath);
	
			Stat stat2 = zkc.exists(addPath, null);
			if(stat2!=null){
				String addP2 = job.getJobpoolPath();
				// add job to the job pool
				createOnePersistentFolder(addP2, job.toJobDataString());

			} else {
				System.out.println(addPath + " does not exist in addToJobPool()");
				return false;
			}
		} else {
			System.out.println(JOBPOOL_PATH + " does not exist in addToJobPool()");
			return false;
		}
		return true;
	}

	/**
	 * Reassign the workload of a worker back to jobpool.
	 * This is called when a worker is removed from the system. The
	 * unfinished work need to be moved to the jobpool.
	 * 
	 * @param removedWorker - name of the worker removed.
	 * @return - true if any work was reassigned, false otherwise.
	 */
	private boolean reassignWork(String removedWorker){
		boolean workWasAssigned = false;
		// Job assigned to the removed worker
		List<String> assignedList = zkc.getChildren(JOBS_PATH + "/" + removedWorker);
		if (assignedList.size() > 0) {
			Iterator l;
			l = assignedList.listIterator();
			while(l.hasNext()){
				String jobName = (String)l.next();
				Stat stat = null;

				String path = JOBS_PATH + "/" + removedWorker + "/" + jobName;
				stat = zkc.exists(path, null); // see if node in jobList still exists in workerList
				if(stat == null){
					// error.. but continue.. 
					System.out.println("Critical Error!! in reassignWork. " + path + " does not exist");
				}else{
					// extract job info
					String data = zkc.getData(path, null, stat);
					if(data != null){
						JobObject j = new JobObject();
						j.parseJobString(data);

						if (DEBUG)	
							System.out.println("Adding into jobpool, data:" + data);

						boolean w = addToJobPool(j);
						if (w) {
							workWasAssigned = true;
						}
						if (DEBUG)
							System.out.println("Deleting job: " + path);
						zkc.delete(path, -1);
					}else{
						// error.. but continue.. 
						System.out.println("Critical Error!! in reassignWork. can not get data");
					}
				}
			}
		}
		return workWasAssigned;
	}
	
	/**
	 * Signal to scheduler that work reassignment happened.
	 * This needs to be done because to prevent scheduler from setting
	 * too many watches, scheduler only has a high level watch on /jobpool
	 * and not on each /jobpool/{jobId}
	 */
	private static void signalWorkReassigned() {
		// signal work reassigned by changing the jobpool data. 
		// scheduler will pick up the change and reschedule
    	Stat stat = null;
    	stat = zkc.exists(JOBPOOL_PATH, null);
    	if(stat == null){
			System.out.println("Critical Error!! in signalWorkReassigned. " + JOBPOOL_PATH + " does not exist");
    		return;
    	}

		String num = zkc.getData(JOBPOOL_PATH, null, stat);
		if(num != null){
			Integer parsed = Integer.parseInt(num);

			Integer newValue;
			newValue = new Integer(parsed.intValue()+1);

			Stat stats = zkc.setData(JOBPOOL_PATH, newValue.toString(), -1);
			if(stats == null){
				System.out.println("Critical Error!! in signalWorkReassigned. " + JOBPOOL_PATH + " can not set new value");
			}
		}else{
			System.out.println("Critical Error!! in signalWorkReassigned. can not get num");
		}
	}


	/**
	 * Create znode for workers under /jobs if they newly joint
	 */
	private void handleJoinedWorkers (){
		List<String> workerList = zkc.getChildren(WORKER_PATH); 

		ListIterator l;
		l=workerList.listIterator();
		
		while(l.hasNext()){
			String workerName = (String)l.next();
			Stat stat = null;
			String path = JOBS_PATH + "/" + workerName;
			stat = zkc.exists(path, null); // see if node in workerList exists in jobList

			if(stat == null){
				//create worker
				createOnePersistentFolder(path, null);
			}
		}
	}

	/**
	 * See if any workers were disconnected from the 
	 * system and reassign work if necessary.
	 */
	private void handleLeftWorkers() {
		// Get all workers under /jobs
		List<String> workerList = zkc.getChildren(JOBS_PATH);
		boolean workReassigned = false;
		ListIterator l;
		l=workerList.listIterator();
		while(l.hasNext()){
			String r = (String)l.next();
			Stat stat = null;
			String rPath = WORKER_PATH + "/" + r;
			stat = zkc.exists(rPath, null); 
			// see if worker node still exists.
			if(stat == null){
				if (DEBUG)
					System.out.println("Worker left...");
				workReassigned = workReassigned || reassignWork(r);
				zkc.delete(JOBS_PATH + "/" + r, -1);
			} 
		}

		if (workReassigned) {
			signalWorkReassigned();
		}
	}

	/**
	 * A change was detected in /worker. Need to balance the
	 * workers under /worker and /jobs
	 */
    private void balanceWorkerDir(){		
		if (DEBUG)
			System.out.println("See if new worker joined...");
		handleJoinedWorkers();

		if (DEBUG)
			System.out.println("See if any workers died...");
		handleLeftWorkers();
	
		//re-enable watch   
		List<String> stats = zkc.getChildren(WORKER_PATH, workerWatcher);

		if(stats == null){		
			System.out.println("ERR: " + WORKER_PATH + " does not exist ");
		}
    }


	/**
	 * Try to be the primary jobtracker by checking and trying 
	 * to create the /jobTracker znode in zookeeper.
     * If become the primary, check if the high level znodes are
	 * already created.
     */
    private void tryToBeBoss() {
        Stat stat = zkc.exists(JOB_TRACKER_PATH, jobTrackerWatcher);
		
		// JOB_TRACKER_PATH doesn't exist, try to be the primary
        if (stat == null) {             
			if (DEBUG) 
	            System.out.println("Creating " + JOB_TRACKER_PATH);
            Code ret = zkc.create(
		        JOB_TRACKER_PATH,       // Path
		        jobTrackerServerInfo,   // information
		        CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
            );

            if (ret == Code.OK) {
				// If we successfully created the jobTracker folder, we are the boss.
				boss =1;
				if (DEBUG)
					System.out.println("Primary jobTracker!");
				createPersistentFolders();
				getCurrentJob();
				balanceWorkerDir();
			}
        } 
    }


	/**
	 * Entry point for jobTracker.
	 * @param args list from user.
	 * arg[0] is the port of zookeeper
	 * arg[1] is the port for job tracker to listen to.
	 */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: sh ./bin/jobTracker.sh zkServer jobTrackerPort");
            return;
        }
		
		// Assume we are backup until we fight for it.
		boss = 0;

		try{
			// create a server socket to listen on
        	serverSocket = new ServerSocket(Integer.parseInt(args[1]));
		}catch (Exception e){
			System.out.println("Failed to create server socker");
			return;
		}		
		
		ZookeeperLocation = args[0];
		String portToListen = args[1];
		String myHostName;

		try{
			myHostName = InetAddress.getLocalHost().getHostName();
		}catch (Exception e){
			System.out.println("Failed to get host name");
			return;
		}		

		jobTrackerServerInfo = myHostName + ":" + portToListen;
		if (DEBUG)
	        System.out.println("Location of jobTracker: "+jobTrackerServerInfo);

        JobTracker t = new JobTracker(ZookeeperLocation);   
		if (DEBUG)
	        System.out.println("Sleeping...");
        t.tryToBeBoss();

        while (boss==0) {
			// just sleep.. zookeeper will notify if the primary disconnected.
            try{ Thread.sleep(1000); } catch (Exception e) {}
        }

        while (boss==1) {
			if (DEBUG)				
				System.out.println("Listening for client connection...");
			try {
				Thread.sleep(2000);
				// create a new thread to handle client connection.
        		new HandleClient(serverSocket.accept()).start();
			} catch (Exception e){
				System.out.println("Failed to create new HandleClient");
			}		   	
        }
    }

}
