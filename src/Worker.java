/*
 * Responsibility for Worker:
 * 1.Run Benchmark of current available node, get speed information for Scheduler
 * 2.Get Hardware information and Benchmark information of the current node
 * 3.Create one instance under workers directory
 * 4.Execute running event triggered and child has changed (
 * 5.After finish running, delete from Jobs
 * 6.Add to result in 'results' (i.e. output result, running time, hardware info, etc)
 * 7.Put this Worker Object to freeWorker
 */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.File;
import java.math.BigInteger;
import java.net.*;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;


public class Worker{	//worker node, need to know hardware configurations
	static ZkConnector zkc;
	static WorkerObject wk;
	static String hostname;
	//static List<Thread> threadList= new List<Thread>();
	static HashMap<String, WorkerThreadHandler> checkMap = new HashMap<String,WorkerThreadHandler>();
	static int index=0;
	static int max_executions=0;
	static int iterator=0;
	static WorkerThreadHandler worker_t[];
    final static String JOB_TRACKER_PATH = "/jobTracker";
	final static String WORKER_PATH = "/worker";
	final static String JOBS_PATH = "/jobs";
	final static String RESULT_PATH = "/result";
	final static String SEQ_PATH = "/seq";
	final static String JOBPOOL_PATH = "/jobpool";
	final static String FREE_WORKERS_PATH = "/freeWorkers";
	ExecutorService executor;
	//Watcher fsWatcher;
	Watcher WorkerWatcher;
	static String Workerid=null;			//workerid of this node(worker)
	String Workerpath=null;
	
	long benchmarkTime = 0;
	long executionTime = 0;
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException, NumberFormatException, ClassNotFoundException {
		if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:zkPort");
            return;
        }
		
		List<String> jobs = null, tasks = null; 
		String myHostName;
		String WorkerServerInfo;
		try{
			myHostName = InetAddress.getLocalHost().getHostName();
		}catch (Exception e){
			System.out.println("Failed to get host name");
            e.printStackTrace();
			return;
		}		
		WorkerServerInfo = myHostName;
		System.out.println( args[0]);
		Worker wk = new Worker(args[0],WorkerServerInfo);
		wk.createPersistentFolders();
		wk.Building();
		System.out.println("Sleeping...");
		while (true) {
		    try{ Thread.sleep(5000); } catch (Exception e) {}
		} 
	 }
	 
	 
	public static void Thread_complete(long execution, int retcode, String currentJob, int threadNum, int Q, String location,int jobID){
			synchronized(zkc){
				if(retcode==0){	
						String WorkerJobPath = JOBS_PATH+"/worker-"+Workerid;	
						System.out.println("finish executing jobs....."+WorkerJobPath+"/"+currentJob);
						zkc.delete(WorkerJobPath+"/"+currentJob,-1);
				}
										
				checkMap.remove(currentJob);
				//TODO:assume now updating result to RESULT_PATH directory
				System.out.println("finish deleting, ready to move on");
				//Update_WorkerObj();	//delimited string   wkid:cpucoreNumber:jobspeed
				String wkInfo = wk.toNodeDataString();
				ResultObject RO = new ResultObject(hostname, location, execution, jobID, Q);
				String resultInfo = RO.toNodeDataString();
				index+=1;

				zkc.create(										//create free worker object
						FREE_WORKERS_PATH+"/"+"worker-"+Workerid+":"+index,       // Path
						wkInfo,   // information
						CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
				);
				System.out.println(jobID+"        " +resultInfo);
				zkc.create(
						RESULT_PATH+"/"+jobID+"/"+RO.get_Result_Node_Name(),
						resultInfo,
						CreateMode.EPHEMERAL
				);
			}	
	}
	
	
	public Worker(String hosts, String WorkerServerInfo){
		//benchmarkTime=benchmark();
		zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
            
			Stat stats = zkc.exists("/worker",null);
			if(stats==null) {						//create worker root directory for the first time
				zkc.getZooKeeper().create("/worker", null, ZkConnector.acl, CreateMode.PERSISTENT);
				System.out.println("/worker created");
			}
			 
			WorkerWatcher = new Watcher(){
	        	 @Override
	             public void process(WatchedEvent event) {
					try{Thread.sleep(5000);}
					catch(Exception e){}
					 
	                 String path = event.getPath();

	                 String WorkerJobPath= JOBS_PATH+"/worker-"+Workerid;
	                 String currentJob="dummy";
	                 String taskinfo=null;
	                 int retcode;
					 System.out.println("getting jobs");
	                 switch (event.getType()){
	                 	case NodeChildrenChanged:
	                 		try {
			             			synchronized(zkc){
										iterator+=1;
					                    //if (path.equals(Workerpath)){
					                    	Stat stat = zkc.exists(WorkerJobPath, null);

					                    	List<String> children=zkc.getChildren(WorkerJobPath);
										JobObject jo = new JobObject();
					                    	for(String child: children){
					                    		System.out.println(child);
					                    		if(checkMap.get(child)==null){
												taskinfo = zkc.getData(WorkerJobPath+"/"+child, null, stat);
					                    			currentJob = child;													
							                		jo.parseJobString(taskinfo);
							                		int Qvalue= jo.nValue;
												int jobID =jo.jobId;
							                		String inputLocation= jo.inputFile;
							                		WorkerThreadHandler t = new WorkerThreadHandler();
							                		t.setVal(inputLocation, Qvalue, currentJob, jobID);
					                    			checkMap.put(currentJob,t);
					                    		}
					                    	}
					                    	
					                  }	
			                        	
			                        	if(taskinfo!=null){
			                        			System.out.println("taskinfo is not null, can execute now");
							                
											//add Future to the list, we can get return value using Future
											System.out.println("waiting or no");
											/*while(iterator<8){
												System.out.println("waiting for all threads coming");
												Thread.sleep(120); 
												
											}*/
											//Thread.sleep(2000);			//wait for everyone coming
											for (String key: checkMap.keySet()) {
												
												executor.submit(checkMap.get(key));
											}
											
			                           }
	                           	
	                        } catch (Exception e) {
	                            e.printStackTrace();
	                        }
	                 	//case NodeDataChanged:
	                 		//;
	                 		
	                 }zkc.getChildren(JOBS_PATH+"/worker-"+Workerid, WorkerWatcher );
                    
	        	 }			
	        };
			
			
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
		
       
	}


	public void Building(){
		//create child directory of worker, assign id to that worker
			try{
				Workerpath = zkc.getZooKeeper().create(WORKER_PATH+"/"+"worker-", null, ZkConnector.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
				String[] temp = Workerpath.split("-");
				Workerid = temp[1];			//create workerid of this worker
				get_Host_Name();
				Create_WorkerObj(Workerid);
				String info = wk.toNodeDataString();
				System.out.println(info);
				for(index=0;index<wk.Node_power();index++){
					System.out.println("creating workerObjects");
					zkc.create(										//create free worker object
			                FREE_WORKERS_PATH+"/"+"worker-"+Workerid+":"+index,       // Path
			                info,   // information
			                CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
			     	);

				}
				this.max_executions= wk.Node_power();
				this.executor = Executors.newCachedThreadPool();
				
				zkc.setData(											//set data for worker
		                    WORKER_PATH+"/"+"worker-"+Workerid,       // Path
		                    info,  // information
							-1
		                    );
				zkc.getChildren(JOBS_PATH+"/worker-"+Workerid, WorkerWatcher );

				}catch(Exception e) {
            			System.out.println("Building Worker: "+ e.getMessage());
        		}
		}



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
		
		createOnePersistentFolder(FREE_WORKERS_PATH, null);
		// create result folder
		createOnePersistentFolder(RESULT_PATH, null);

		// create jobpool folder
		createOnePersistentFolder(JOBPOOL_PATH, null);
    }


	public void get_Host_Name(){
		try{
			Process p = Runtime.getRuntime().exec("hostname");
				p.waitFor();		//create shell object and retrieve cpucore number
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			this.hostname=br.readLine();
		}catch (Exception e){
			e.printStackTrace();
		}
	}


	public void Update_WorkerObj(){
		try{
			Process p = Runtime.getRuntime().exec("sh ../src/cpu_core.sh");
    			p.waitFor();		//create shell object and retrieve cpucore number
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			wk.cpucore = br.readLine();
			wk.executionTime= this.executionTime;
			
		
		}catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	
	private void Create_WorkerObj(String wkid){
			String wkname= "worker-"+wkid;
			wk= new WorkerObject(wkname);
			Update_WorkerObj();
	}
	
	

	
	
}
