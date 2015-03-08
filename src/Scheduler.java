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
 * Responsibilities of scheduler:
 * 1. Schedule jobs to workers by calling scheduleJobs() in ScheduleAlgo.java
 * 2. Re-schedule jobs on changes in workers or jobs
 * 3. Assign jobs to free workers  
 *
 * @author Jie (Jacky) Liu, jjiiee.liu@mail.utoronto.ca
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
	// Watcher on jobpool directory
 	Watcher jobpoolWatcher;
	// watcher on free worker directory
	Watcher freeWorkerWatcher;

	static String schedulerServerInfo;

    final static String SCHEDULER_PATH = "/scheduler";
	final static String WORKER_PATH = "/worker";
	final static String FREE_WORKERS_PATH = "/freeWorkers";
	final static String JOBS_PATH = "/jobs";
	final static String JOBPOOL_PATH = "/jobpool";

	final static boolean DEBUG = true;

	/** 
     * constructor for job tracker
	 * @param host
     */
    public Scheduler(String zooLocation) {
		if (DEBUG)		
	 		System.out.println("constructing scheduler: " + zooLocation);
        zkc = new ZkConnector();
        try {
            zkc.connect(zooLocation);
        } catch(Exception e) {
            System.out.println("Zookeeper can not connect "+ e.getMessage());
        }

		if (DEBUG)
	 		System.out.println("Zookeeper connected");

		jobQueue = null;
		workersList = new ArrayList<WorkerObject>();
		jobsList = new ArrayList<JobObject>();
		knownJobIds = new ArrayList<String>();
		
		// initialize watchers
		// Watch to become the primary scheduler
        schedulerWatcher = new Watcher() { 
		    @Override
		    public void process(WatchedEvent event) {
				if (DEBUG)
					System.out.println("--- In scheudlerWatcher ---");

				// Try to be primary
				EventType type = event.getType();
				if (type == EventType.NodeDeleted) {
					if (DEBUG)
						System.out.println("scheduler deleted! Let's go!");       
					tryToBeBoss();
				}

				// re enable watch 
				zkc.exists(SCHEDULER_PATH, schedulerWatcher);
		    }
		};
        // watch for new jobs.     
        jobpoolWatcher = new Watcher() { // Anonymous Watcher
			@Override
			public void process(WatchedEvent event) {
				if (DEBUG)
					System.out.println("--- In jobpoolWatcher ---");
				EventType type = event.getType();
				if ((type == EventType.NodeChildrenChanged) 
					|| type == EventType.NodeDataChanged) {
					if (DEBUG)
						System.out.println("job added in jobpool. Now handle it.");       

					if (DEBUG)
						System.out.println("Sleep to debounce .... ");     
					try {
						Thread.sleep(5000);
					} catch (Exception e) {};

					updateJobsList();
					updateWorkersList();
					doSchedule();
					// may alreay have been free workers, so try to assign jobs
					attemptToAssignJobs();
				}        

				// re enable watch
				if (type == EventType.NodeDataChanged) {
					Stat dataStat = null;
					zkc.getData(JOBPOOL_PATH, jobpoolWatcher, null);
				} else {
					zkc.getChildren(JOBPOOL_PATH, jobpoolWatcher);
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
				if (type == EventType.NodeChildrenChanged) {
					if (DEBUG)
						System.out.println("Workers changed. Now handle it.");       

					if (DEBUG)
						System.out.println("Sleep to debounce .... ");     
					try {
						Thread.sleep(5000);
					} catch (Exception e) {;};
					updateWorkersList();
					updateJobsList();
					doSchedule();
					// may alreay have been free workers, so try to assign jobs
					attemptToAssignJobs();
				}          
				// re enable watch               
				zkc.getChildren(event.getPath(), workerWatcher);
		    } 
		};
		// watch for freeworker changes
		freeWorkerWatcher = new Watcher() { // Anonymous Watcher
		    @Override
		    public void process(WatchedEvent event) {
				if (DEBUG)
					System.out.println("--- In freeWorkerWatcher ---");
				EventType type = event.getType();
				if (type == EventType.NodeChildrenChanged) {
					if (DEBUG)
						System.out.println("Free worker changed. Now handle it.");       			
					attemptToAssignJobs();
				}   
				// re enable watch
				List<String> s = zkc.getChildren(FREE_WORKERS_PATH, freeWorkerWatcher);
		    } 
		};
    }

	/**
	 * Attempt to assign jobs to all the free workers by moving
	 * job nodes from /jobpool/{jobId} to /jobs/{workerName}
	 */
	private void attemptToAssignJobs() {
		if (DEBUG)
			System.out.println("Attempting to assign jobs..."); 
		// Get all free workers
		List<String> freeWorkerNodes = zkc.getChildren(FREE_WORKERS_PATH);
		if (freeWorkerNodes == null || freeWorkerNodes.size() == 0) {
			// no free workers, return
			return;
		}

		// NOTE: Because zookeeper does not allow duplicated node names
		// 		 and a worker could have more than one free capacity, the 
		// 		 name of the freeworker nodes are not the name of the workers.

		List<WorkerObject> freeWorkers = new ArrayList<WorkerObject>();

		// Get the worker object of free workers
		ListIterator fwnIterator = freeWorkerNodes.listIterator();
		while(fwnIterator.hasNext()) {
			String freeNodeName = (String)fwnIterator.next();
			Stat workerStat = null;
			String path = FREE_WORKERS_PATH + "/" + freeNodeName;
			workerStat = zkc.exists(path, null);
			if(workerStat != null){
				String workerData = zkc.getData(path, null, workerStat);
				WorkerObject wo = new WorkerObject();
				wo.parseNodeString(workerData);
				wo.freeWorkerNodeName = freeNodeName;
				freeWorkers.add(wo);
			}
		}

		if(freeWorkers.size() == 0){
			if (DEBUG)
				System.out.println("No free workers"); 
			return;
		}

		if (DEBUG)
			System.out.println("Assigning jobs");   

		// Only do reassignment once to prevent scheduling
		// from being triggered too many times when the job
		// is close to completion.
		boolean reassignmentDone = false;

		ListIterator l = freeWorkers.listIterator();
		while(l.hasNext()) {
			WorkerObject wo = (WorkerObject)l.next();
			String workerName = wo.getNodeName();
			JobObject j = getJob(workerName);

			// only reschedule once 
			if (j == null && !reassignmentDone){
				doSchedule();
				reassignmentDone = true;
			}

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
							// remove the assigned job from job list
							jobsList.remove(j);
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
	}

	/**
	 * Get the next job assigned to the worker from the job queue
	 *
	 * @param workerName - name of the worker to get job.
	 * @return - JobObject of the next job assignment for the worker
	 */
	private JobObject getJob(String workerName) {
		JobObject j = null;
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

	/**
	 * Performe scheduling with the current jobs and workers lists
	 * by calling scheduleJobs() in ScheduleAlgo.java
	 */
	private void doSchedule() {
		if (DEBUG)
			System.out.println("Scheduling jobs");  
 
		if (workersList.size() > 0 && jobsList.size() > 0) {
			List<JobObject> jobListCopy = new ArrayList<JobObject>();

			// Create a deep copy of jobList so the jobsList
			// is not emptied after scheduling.

			// NOTE: we want to keep the reference to jo the
			//		 same as the original list for removal
			// 		 from jobsList uppon assignment

			for(JobObject jo: jobsList) {
				jobListCopy.add(jo);
			}

			// Pass in additional information if necessary.
			jobQueue = ScheduleAlgo.scheduleJobs(workersList, jobListCopy); 
		} else {
			// no jobs
			jobQueue = null;
		}
	}

	/**
	 * Update jobsList to all unstarted jobs in /jobpool.
	 */
	private void updateJobsList() {
		if (DEBUG)
			System.out.println("Updating jobs list");   
		Stat stat = zkc.exists(JOBPOOL_PATH, null);
		List<String> ZJobIdList;
		List<String> ZJobList;
		if(stat != null){		
			// clear and regenerate the job list.
			this.jobsList.clear();

			ZJobIdList = zkc.getChildren(JOBPOOL_PATH);

			if (ZJobIdList.size() == 0) {
				if (DEBUG)
					System.out.println("jobpool has no jobs");
				return;
			}

			ListIterator l;
			l = ZJobIdList.listIterator();
			// go through all /jobpool/{jobId}
			while(l.hasNext()){

				String jobId = (String)l.next();
				Stat jobIdStat = null;
				String idPath = JOBPOOL_PATH + "/" + jobId;
				jobIdStat = zkc.exists(idPath, null);

				if(jobIdStat != null){

					ZJobList = zkc.getChildren(idPath);
					if (ZJobList.size() == 0) {
						if (DEBUG)
							System.out.println("jobid " + idPath + " has no job ids");
						continue;
					}
					ListIterator jl;
					jl = ZJobList.listIterator();
					// go through all /jobpool/{jobId}/{jobs}
					while(jl.hasNext()){
						String jobIdName = (String)jl.next();
						Stat jobStat = null;
						String rPath = idPath + "/" + jobIdName;

						jobStat = zkc.exists(rPath, null); 

						if (jobStat != null) {
							String jobData = zkc.getData(rPath, null, jobStat);

							JobObject jo = new JobObject();
							jo.parseJobString(jobData);
							if (DEBUG)
								System.out.println("Adding new job to joblist; data: " + jobData);
							this.jobsList.add(jo);
						}
					}
				}
			}
		} else { 
			System.out.println("jobpool does not exist"); 
		}
		if (DEBUG)
			System.out.println("Joblist now has " + this.jobsList.size() + " jobs");     
	}


	/**
	 * Update workersList to all workers in /worker
	 */
	private void updateWorkersList() {
		if (DEBUG)
			System.out.println("Updating workers list");   
		Stat stat = zkc.exists(WORKER_PATH, null);
		List<String> ZWorkerList = null;

		if(stat != null){		
			// clear and regenerate the worker list.
			this.workersList.clear();

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
		if (DEBUG)
			System.out.println("WorkersList now has " + this.workersList.size() + " workers");     
	}


	/**
	 * Create a persistent znode(folder) with the specified path and value.
	 *
	 * @param path - path of the znode to be created
	 * @param value - value to be stored in the znode
	 */
    private static synchronized Code createOnePersistentFolder(String Path, String value){	
		// create folder
		Code ret = null;
 		Stat stat = zkc.exists(Path,null);
        if (stat == null) { 
			if (DEBUG)
		        System.out.println("Creating " + Path);
	        ret = zkc.create(
		        Path,         // Path of znode
		        value,        // Data
		        CreateMode.PERSISTENT   // Znode type, set to PERSISTENT.
            );
	        if (ret == Code.OK) {
				if (DEBUG)
					System.out.println(Path.toString()+" path created!");
	   	 	} else {
				System.out.println(Path.toString()+" path creation failed!");
			}
        }
		return ret;
    }

	/**
	 * Create persistent necessary to set watches.
 	 */
    private void createPersistentFolders(){
		// create jobs folder
		createOnePersistentFolder(JOBS_PATH, null);

		// create worker folder
		createOnePersistentFolder(WORKER_PATH, null);

		// create worker folder
		createOnePersistentFolder(FREE_WORKERS_PATH, null);

		// create jobpool folder
		createOnePersistentFolder(JOBPOOL_PATH, "1");
    }

	/**
	 * Try to be the primary scheduler by checking and trying
	 * to create the scheduler dir. If became primary, check 
	 * if the necessary paths are already created and attempt
	 * to assign jobs.
     */
    private void tryToBeBoss() {
        Stat stat = zkc.exists(SCHEDULER_PATH, schedulerWatcher);
		
		// SCHEDULER_PATH doesn't exist; let's try creating it
        if (stat == null) {            
			if (DEBUG)  
	            System.out.println("Creating " + SCHEDULER_PATH);
            Code ret = zkc.create(
		        SCHEDULER_PATH,       // Path
		        schedulerServerInfo,   // information
		        CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
            );

            if (ret == Code.OK) {
				// If we successfully created the scheduler folder, we are the primary.
				boss =1;
				if (DEBUG)
					System.out.println("Primary Scheduler!");
				createPersistentFolders();
				
				updateWorkersList();
				updateJobsList();
				doSchedule();
				attemptToAssignJobs();

				// Set watches
				// set worker watch
				zkc.getChildren(WORKER_PATH, workerWatcher);
				// set free worker watch
				zkc.getChildren(FREE_WORKERS_PATH, freeWorkerWatcher);
				// set jobpool watch - would be triggered when a new job id is created.
				zkc.getChildren(JOBPOOL_PATH, jobpoolWatcher);
				// set jobpool watch - would be triggered when job is reassigned. 
				// (In case of signalWorkReassigned() from jobtracker)
				zkc.getData(JOBPOOL_PATH, jobpoolWatcher, null);  

				if (DEBUG)
					System.out.println("Watches set");
			}
        } 
    }

	/**
	 * Get the jobs currently executing by each worker. 
	 * Returns a hashtable of a list of jobs currenly executing by the worker, keyed by worker names.
	 *
	 * NOTE: This is currently not used but may be helpful
	 * 		 to have for future scheduling algo.
	 *		(Not well tested)
	 *
	 * @return - a hashtable of lists of JobObjects keyed by worker names
	 */
	private Hashtable<String, List<JobObject>> getCurrentWorkerJobs() {
		// get jobs currently being worked on by each worker
		Hashtable<String, List<JobObject>> currentWorkerJobs = new Hashtable<String, List<JobObject>>();
		for(WorkerObject wo: workersList) {
			String woName = wo.getNodeName();
			if (woName != null) {
				String path = JOBS_PATH + "/" + woName;
				Stat stat = zkc.exists(path, null);
				// if the worker exists under the jobs path
				if (stat != null) {
					List<JobObject> curJobList = new ArrayList<JobObject>();
					// get all the jobs it is currently working on
					List<String> jobChildren = zkc.getChildren(path);
					Iterator jc = jobChildren.listIterator();
					while(jc.hasNext()){
						String jobName = (String)jc.next();
						String workerJobPath = path + "/" + jobName;
						stat = zkc.exists(workerJobPath, null); 

						if (stat != null) {
							// get the info of the job currently working on 
							String jobData = zkc.getData(workerJobPath, null, stat);

							JobObject jo = new JobObject();
							jo.parseJobString(jobData);
					
							curJobList.add(jo);
						}
					}

					if (curJobList.size() > 0) {
						currentWorkerJobs.put(woName, curJobList);
					} else {
						currentWorkerJobs.put(woName, null);
					}
				} else {
					// currently no job
					currentWorkerJobs.put(woName, null);
				}
			} else {
				System.out.println("worker object does not have name");
			}
		}
		return currentWorkerJobs;
	}


	/**
	 * Entry point for scheduler.
	 * arg[0] is the port of zookeeper
	 */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: sh ./bin/scheduler.sh zkServer");
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
		if (DEBUG)
	        System.out.println("Location of jobTracker: "+schedulerServerInfo);

		// create scheduler
        Scheduler s = new Scheduler(zookeeperLocation);   
        s.tryToBeBoss();

		if (boss == 0 && DEBUG) {
			System.out.println("Sleeping...");
		}

        while (boss==0) {
            try{
				Thread.sleep(1000); 
			} catch (Exception e) {;}
        }

		if (DEBUG)
			System.out.println("Waiting for task...");
	    while (boss==1) {
			try{
				Thread.sleep(2000);        		
			}catch (Exception e){;}		   	
        }
    }
}
