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
import java.util.HashMap;
import java.util.Map;
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
	static HashMap<String, String> checkMap = new HashMap<String,String>();
    final static String JOB_TRACKER_PATH = "/jobTracker";
	final static String WORKER_PATH = "/worker";
	final static String JOBS_PATH = "/jobs";
	final static String RESULT_PATH = "/result";
	final static String SEQ_PATH = "/seq";
	final static String JOBPOOL_PATH = "/jobpool";
	final static String FREE_WORKERS_PATH = "/freeWorkers";

	//Watcher fsWatcher;
	Watcher WorkerWatcher;
	String Workerid=null;			//workerid of this node(worker)
	String Workerpath=null;
	
	long benchmarkTime = 0;
	long executionTime = 0;
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException, NumberFormatException, ClassNotFoundException {
		if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:zkPort");
            return;
        }
		
		/*try{
			// create a server socket to listen on
        	serverSocket = new ServerSocket(Integer.parseInt(args[1]));
		}catch (Exception e){
			System.out.println("Failed to create server socker");
			return;
		}	*/
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
	                 String path = event.getPath();

	                 String WorkerJobPath= JOBS_PATH+"/woker-"+Workerid;
	                 String currentJob="dummy";
	                 String taskinfo=null;

	                 switch (event.getType()){
	                 	case NodeChildrenChanged:
	                 		try {
	                            //if (path.equals(Workerpath)){
	                            	Stat stat = zkc.exists(WorkerJobPath, null);

	                            	 List<String> children=zkc.getChildren(WorkerJobPath, WorkerWatcher);
	                            	for(String child: children){
	                            		if(checkMap.get(child)==null){
	                            			currentJob = child;
	                            			checkMap.put(child,"occupied");
			                        		taskinfo = zkc.getData(WorkerJobPath+"/"+child, null, stat);
			                        		break;
	                            		}
	                            	}
	                            	
	                            	
	                            	if(taskinfo!=null){
					                    	JobObject jo = new JobObject();
					                    	jo.parseJobString(taskinfo);
					                    	int Qvalue= jo.nValue;
					                    	String inputLocation= jo.inputFile;
							         		long startTime = System.nanoTime();
							         			
					                    	try{ 
												//mock of execution, depends on where we put zookeeper and NPAIRS executables we can change shell command 
												String command = "sh ../execute/execute.sh " + inputLocation+" "+ Qvalue;				
												Process p = Runtime.getRuntime().exec(command);
												if(p.waitFor()==0)		
													zkc.delete(WorkerJobPath,-1);
										
									
											} catch (Exception e) {
												e.printStackTrace();
											}	//TODO:assume this is running the child node for now
					                    	 	
					                    	long endTime = System.nanoTime();	                            		
							         		executionTime = (endTime - startTime);
					                    	checkMap.remove(currentJob);
					                    		//TODO:assume now updating result to RESULT_PATH directory
					                    		
											Update_WorkerObj();	//delimited string   wkid:cpucoreNumber:jobspeed
											String info = wk.toNodeDataString();
					                    		zkc.setData(
					                                    FREE_WORKERS_PATH+":"+Workerid,       // Path
					                                    info,   // information
					                                    -1
					                                    );
	                               }
	                            		
	                        } catch (Exception e) {
	                            e.printStackTrace();
	                        }
	                 	//case NodeDataChanged:
	                 		//;
	                 		
	                 }
                     zkc.getChildren(JOBS_PATH+"/worker-"+Workerid, WorkerWatcher );
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
				
				Create_WorkerObj(Workerid);
				String info = wk.toNodeDataString();
				//System.out.println(info);
				for(int i=0;i<wk.Node_power();i++){
					zkc.create(										//create free worker object
			                FREE_WORKERS_PATH+"/"+"worker-"+Workerid+":"+i,       // Path
			                info,   // information
			                CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
			     	);

				}
				
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



	public void Update_WorkerObj(){
		try{
			Process p = Runtime.getRuntime().exec("sh cpu_core.sh");
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
	
	
	
	private long benchmark(){		//mock of benchmark
		long startTime = System.nanoTime();
		try{ Thread.sleep(1000); } catch (Exception e) {}
		long elapsedTime = System.nanoTime() - startTime;
		return elapsedTime;
	}
	
	
}
