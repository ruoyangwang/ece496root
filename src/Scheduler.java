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

public class Scheduler {

    private static ZkConnector zkc;
	// Whether we are the primary or backup job tracker
	static int boss;

	private ArrayList<WorkerObject> workersList;
	private ArrayList<JobObject> jobsList;
	private Hashtable<String, Queue<JobObject>> jobQueue;

	private ArrayList<String> knownJobIds;

    // watcher for primary/backup of job tracker
	Watcher schedulerWatcher;
	// Watcher on workers directory
 	Watcher workerWatcher;
	// Watcher on pobpool directory
 	Watcher jobpoolWatcher;
	// watcher on free worker directory
	Watcher freeWorkerWatcher;

	static String schedulerServerInfo;

    final static String SCHEDULER_PATH = "/scheduler";
	final static String WORKER_PATH = "/worker";
	final static String FREE_WORKERS_PATH = "/freeWorkers";
	final static String JOBS_PATH = "/jobs";
	final static String JOBPOOL_PATH = "/jobpool";

	/** 
     * constructor for job tracker
     */
    public Scheduler(String hosts) {

 		System.out.println("constructing job tracker: " + hosts);
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper can not connect "+ e.getMessage());
        }
 		System.out.println("Zookeeper connected");

		jobQueue = null;
		workersList = new ArrayList<WorkerObject>();
		jobsList = new ArrayList<JobObject>();
		knownJobIds = new ArrayList<String>();
		
		// initialize watchers
        schedulerWatcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
								System.out.println("");
								System.out.println("--- In scheudlerWatcher ---");
								// Try to be 
								EventType type = event.getType();
								if (type == EventType.NodeDeleted) {
									System.out.println("scheduler deleted! Let's go!");       
									tryToBeBoss();
								}
								// re enable watch 
								Stat stat = zkc.exists(SCHEDULER_PATH, schedulerWatcher);
								/* This might not be needed.								
								if (type == EventType.NodeCreated) {
									System.out.println(myPath + " created!");       
									tryToBeBoss();
								}*/

                            } };
                            
        jobpoolWatcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
								System.out.println("");
								System.out.println("--- In jobpoolWatcher ---");
								EventType type = event.getType();
								boolean operationNeeded = false;

								/*if (type == EventType.NodeChildrenChanged) {
									List<String> childList = zkc.getChildren(JOBPOOL_PATH, null);
									if (!knownJobIds.containAll(childList)) {
										operationNeeded = true;
										knownJobIds = childList;
									}
								}*/
								operationNeeded = true;


								if ((type == EventType.NodeChildrenChanged && operationNeeded) 
									|| type == EventType.NodeDataChanged) {
									System.out.println("job added in jobpool. Now handle it.");       
									updateJobsList();
									updateWorkersList();
									doSchedule();
									attemptToAssignJobs();
								}        
								// re enable watch
								if (type == EventType.NodeDataChanged) {
									Stat dataStat = null;
									String data = zkc.getData(JOBPOOL_PATH, jobpoolWatcher, dataStat);
								} else {
									List<String> stat = zkc.getChildren(JOBPOOL_PATH, jobpoolWatcher);
								}                 
								//System.out.println("jobpoolWatcher set, eventType: " + event.getType().toString());
                            } };

		workerWatcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
								System.out.println("");
								System.out.println("--- In workerWatcher ---");
								EventType type = event.getType();
								if (type == EventType.NodeChildrenChanged) {
									System.out.println("Workers changed. Now handle it.");       
									updateWorkersList();
									updateJobsList();
									doSchedule();

									attemptToAssignJobs();
								}          
								// re enable watch               
								List<String> stat = zkc.getChildren(event.getPath(), workerWatcher);
								//System.out.println("workerWatcher set");
                            } };

		freeWorkerWatcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
								System.out.println("");
								System.out.println("--- In freeWorkerWatcher ---");
								EventType type = event.getType();
								if (type == EventType.NodeChildrenChanged) {
									System.out.println("Free worker changed. Now handle it.");       
									
									attemptToAssignJobs();
								}   
								// re enable watch
								List<String> s = zkc.getChildren(FREE_WORKERS_PATH, freeWorkerWatcher);
								//System.out.println("freeWorkerWatcher set");
                            } };
    }

	private void removeIfNoChildren(String path) {
		List<String> l = zkc.getChildren(path);
		if (l != null && l.size() == 0) {
			zkc.delete(path, -1);
		}
	}

	private void removeFromJobList(List<JobObject> remove) {
		jobsList.removeAll(remove);
	}

	private void attemptToAssignJobs() {
		System.out.println("Attempting to assign jobs..."); 
		List<String> workerNames = zkc.getChildren(FREE_WORKERS_PATH);
		if (workerNames != null && workerNames.size() > 0) {
			assignJobs(workerNames);
		}
    }

	private void assignJobs (List<String> freeWorkerNodes) {
		//System.out.println("Trying to assign jobs..."); 
		// worker node names are passed in, try to get the names 
		// of each nodes, which are not necessary the same as the node names.
		List<WorkerObject> freeWorkerNames = new ArrayList<WorkerObject>();
		if (freeWorkerNodes != null && freeWorkerNodes.size() > 0) {
		
			ListIterator freeWorkerNodesIterator = freeWorkerNodes.listIterator();
			while(freeWorkerNodesIterator.hasNext()) {
				String freeNodeName = (String)freeWorkerNodesIterator.next();
				Stat workerStat = null;
				String rPath = FREE_WORKERS_PATH + "/" + freeNodeName;
				workerStat = zkc.exists(rPath, null);
				if(workerStat != null){
					String workerData = zkc.getData(rPath, null, workerStat);
					WorkerObject wo = new WorkerObject();
					wo.parseNodeString(workerData);
					wo.freeWorkerNodeName = freeNodeName;
					freeWorkerNames.add(wo);
				}
			}
		}


		if(freeWorkerNames.size() <= 0){
			System.out.println("No Jobs to assign"); 
			return;
		}

		System.out.println("Assigning jobs");   
		ListIterator l;
		l = freeWorkerNames.listIterator();
		List<JobObject> removedJobs = new ArrayList<JobObject>();

		while(l.hasNext()) {
			WorkerObject wo = (WorkerObject)l.next();
			String workerName = wo.getNodeName();
			JobObject j = getJob(workerName);
			if (j != null) {
				// try to add job to worker directory
				String workerPath = JOBS_PATH + "/" + workerName;
				Stat ws = zkc.exists(workerPath, null);
				if (ws != null) {
					// see if job exists in job pool
					String jobPath = j.getJobpoolPath();
					Stat js = zkc.exists(jobPath, null);
					if (js != null) {
						// add job to worker directory
						Code ret = createOnePersistentFolder(workerPath + "/" + j.getJobNodeName(), j.toJobDataString());

						// remove job from job pool
						if (ret == Code.OK) {
							String freeWorkerNodePath = FREE_WORKERS_PATH + "/" + wo.freeWorkerNodeName;
							zkc.delete(freeWorkerNodePath, -1);
							zkc.delete(jobPath, -1);
							// let jobtracker do this
							//removeIfNoChildren(j.getJobpoolParentPath());

							// remove from jobsList
							removedJobs.add(j);
						}
					} else {
						System.out.println("ERR: Failed to assign job. Job dir: " + jobPath + " does not exist");  
					}
				} else {
					System.out.println("ERR: Failed to assign job. Worker dir: " + workerPath + " does not exist");  
				}
			} else {
				System.out.println("No jobs for worker: " + workerName);  
			}

		}

		if (removedJobs.size() > 0) {
			removeFromJobList(removedJobs);
		}
	}

	private JobObject getJob(String workerName) {
		JobObject j = null;
		// if there is job.
		if (jobQueue != null) {
			Queue<JobObject> q = jobQueue.get(workerName);
			if (q != null) {
				if (q.size() > 0) {
					j = q.remove();
				}
			} else {
				System.out.println("ERR: Can not find worker name " + workerName + " in job queue");     
			}		
		}
		return j;
	}


	private void doSchedule() {
		System.out.println("Scheduling jobs");   
		if (workersList.size() > 0 && jobsList.size() > 0) {
			// scheduler may depend on current state of the workers as well ??
			jobQueue = ScheduleAlgo.scheduleJobs(workersList, jobsList); 
		} else {
			// no jobs
			jobQueue = null;
		}
	}

	// Update the jobs list.
	private void updateJobsList() {
		System.out.println("Updating jobs list");   
		Stat stat = zkc.exists(JOBPOOL_PATH, null);
		List<String> ZJobIdList;
		List<String> ZJobList;
		if(stat != null){		
			// clear and regenerate the worker list.
			this.jobsList.clear();

			System.out.println("Sleep to debounce .... ");     

			try {
				Thread.sleep(5000);
			} catch (Exception e) {};

			ZJobIdList = zkc.getChildren(JOBPOOL_PATH);

			if (ZJobIdList.size() == 0) {
				System.out.println("jobpool has no job ids");
				return;
			}

			ListIterator l;
			l = ZJobIdList.listIterator();
		
			while(l.hasNext()){

				String jobId = (String)l.next();
				Stat jobIdStat = null;
				String idPath = JOBPOOL_PATH + "/" + jobId;
				jobIdStat = zkc.exists(idPath, null);

				if(jobIdStat != null){

					ZJobList = zkc.getChildren(idPath);

					if (ZJobList.size() == 0) {
						System.out.println("jobid " + idPath + " has no job ids");
						continue;
					}
					ListIterator jl;
					jl = ZJobList.listIterator();
					while(jl.hasNext()){
						String jobIdName = (String)jl.next();
						Stat jobStat = null;
						String rPath = idPath + "/" + jobIdName;

						jobStat = zkc.exists(rPath, null); 

						if (jobStat != null) {
							String jobData = zkc.getData(rPath, null, jobStat);

							JobObject jo = new JobObject();
							jo.parseJobString(jobData);
							System.out.println("Adding new job to joblist; data: " + jobData);
							this.jobsList.add(jo);
						}
					}
				}
			}
		} else { System.out.println("jobpool does not exist"); }
		System.out.println("Joblist now has " + this.jobsList.size() + " jobs");     
	}


	// Update the workers list.
	private void updateWorkersList() {
		System.out.println("Updating workers list");   
		Stat stat = zkc.exists(WORKER_PATH, null);
		List<String> ZWorkerList = null;

		if(stat != null){		
			// clear and regenerate the worker list.
			this.workersList.clear();

			System.out.println("Sleep to debounce .... ");     

			try {
				Thread.sleep(5000);
			} catch (Exception e) {};

			ZWorkerList = zkc.getChildren(WORKER_PATH, null);

			ListIterator l;
			l = ZWorkerList.listIterator();
		
			while(l.hasNext()){

				String workerName = (String)l.next();
				Stat workerStat = null;
				String rPath = WORKER_PATH + "/" + workerName;
				workerStat = zkc.exists(rPath, null); // see if node in workerList exists in jobList

				if(workerStat != null){
					String workerData = zkc.getData(rPath, null, workerStat);

					WorkerObject wo = new WorkerObject(workerName);
					wo.parseNodeString(workerData);

					this.workersList.add(wo);
				}
			}
		}

		System.out.println("WorkersList now has " + this.workersList.size() + " workers");     
	}


	/**
	 * Create a persistent foler with the specified path and value.
	 */
    private static synchronized Code createOnePersistentFolder(String Path, String value){	
		// create folder
		Code ret = null;
 		Stat stat = zkc.exists(Path,null);
        if (stat == null) { 
	        System.out.println("Creating " + Path);
	        ret = zkc.create(
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
		return ret;
    }

	/**
	 * Create persistent folders.
 	 */
    private void createPersistentFolders(){
		// create jobs folder
		createOnePersistentFolder(JOBS_PATH, null);

		// create worker folder
		createOnePersistentFolder(WORKER_PATH, null);

		// create worker folder
		createOnePersistentFolder(FREE_WORKERS_PATH, null);

		// create jobpool folder
		createOnePersistentFolder(JOBPOOL_PATH, null);
    }

	/**
	 * Try to be the boss by checking and trying to create the jobTracker dir
     * Check if the necessary paths are already created.
     */
    private void tryToBeBoss() {
        Stat stat = zkc.exists(SCHEDULER_PATH, schedulerWatcher);
		
		// SCHEDULER_PATH doesn't exist; let's try creating it
        if (stat == null) {              
            System.out.println("Creating " + SCHEDULER_PATH);
            Code ret = zkc.create(
                        SCHEDULER_PATH,       // Path
                        schedulerServerInfo,   // information
                        CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
                        );

            if (ret == Code.OK) {
				// If we successfully created the jobTracker folder, we are the boss.
				boss =1;

				System.out.println("Primary Scheduler!");
				createPersistentFolders();
				
				updateWorkersList();
				updateJobsList();
				doSchedule();
			
				List<String> freeWorkerNodes = zkc.getChildren(FREE_WORKERS_PATH);
				if (freeWorkerNodes != null && freeWorkerNodes.size() > 0) {			
					assignJobs(freeWorkerNodes);	
				}

				// Set watches
				List<String> s = zkc.getChildren(WORKER_PATH, workerWatcher);
				s = zkc.getChildren(FREE_WORKERS_PATH, freeWorkerWatcher);
				s = zkc.getChildren(JOBPOOL_PATH, jobpoolWatcher);
				Stat dataStat = null;
				String ss = zkc.getData(JOBPOOL_PATH, jobpoolWatcher, dataStat);  
				System.out.println("Watches set");
				System.out.println("");
			}
        } 
    }


	// Main function for job tracker
	// arg[0] is the port of zookeeper
	// arg[1] is the port for job tracker to listen to.
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Scheudler zkServer:zkPort");
            return;
        }
		
		// Assume we are backup until we fight for it.
		boss =0;
		
		String zookeeperLocation = args[0];
		String myHostName;

		try{
			myHostName = InetAddress.getLocalHost().getHostName();
		}catch (Exception e){
			System.out.println("Failed to get host name");
			return;
		}		

		schedulerServerInfo = myHostName;
        System.out.println("Location of jobTracker: "+schedulerServerInfo);

        Scheduler s = new Scheduler(zookeeperLocation);   
		

        s.tryToBeBoss();

		if (boss == 0) {
			System.out.println("Sleeping...");
		}
        while (boss==0) {
            try{
//				System.out.println("Sleeping..."); 
				Thread.sleep(1000); 
			} catch (Exception e) {}
        }


		System.out.println("Watching...");
		System.out.println("");
        while (boss==1) {
			//System.out.println("Watching...");
			try{
				Thread.sleep(5000);        		
			}catch (Exception e){}		   	
        }
		
    }

}
