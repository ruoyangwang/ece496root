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

    final static String JOB_TRACKER_PATH = "/jobTracker";
	final static String WORKER_PATH = "/worker";
	final static String JOBS_PATH = "/jobs";
	final static String RESULT_PATH = "/result";
	final static String JOBPOOL_PATH = "/jobpool";
	final static String FREE_WORKERS_PATH = "/freeWorkers";

	//Watcher fsWatcher;
	Watcher WorkerWatcher;
	String Workerid=null;			//workerid of this node(worker)
	String Workerpath=null;
	String[] hardware_info;
	long core=0, mem_total_JVM, mem_free, mem_max, mem_cur_JVM;
	long maxMemory;
	long benchmarkTime;
	long executionTime;
	
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
		Worker worker = new Worker(args[0],WorkerServerInfo);

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
	                 String Workerpath= JOBS_PATH+"/"+Workerid;
	                 switch (event.getType()){
	                 	case NodeChildrenChanged:
	                 		try {
	                            //if (path.equals(Workerpath)){
			                 		long startTime = System.nanoTime();
			                 			
	                            	 	try{ 
										//mock of execution, depends on where we put zookeeper and NPAIRS executables we can change shell command 
										String command = "sleep 10";				
										Process p = Runtime.getRuntime().exec(command);
										p.waitFor();		//create shell object and retrieve cpucore number
										BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));


									
										//Thread.sleep(1000); 
									} catch (Exception e) {
										e.printStackTrace();
									}	//TODO:assume this is running the child node for now
	                            	 	
	                            	 	long endTime = System.nanoTime();	                            		
			                 		executionTime = (endTime - startTime);
	                            	 	//now need to delete the directory
	                            		zkc.delete(Workerpath,-1);
	                            		
	                            		//TODO:assume now updating result to RESULT_PATH directory
	                            		String info = Create_WorkerObj(Workerid);	//delimited string   wkid:cpucoreNumber:jobspeed
	                            		zkc.create(
	                                            FREE_WORKERS_PATH+"/"+Workerid,       // Path
	                                            info,   // information
	                                            CreateMode.EPHEMERAL  	// Znode type, set to EPHEMERAL.
	                                            );//zkc.getZooKeeper().create("/freeWorkers/"+Workerid, info, ZkConnector.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
	                           // }
	                            		
	                        } catch (Exception e) {
	                            e.printStackTrace();
	                        }
	                 	//case NodeDataChanged:
	                 		//;
	                 		
	                 }
                     zkc.getChildren(JOBS_PATH+"/worker-"+Workerid, WorkerWatcher );
	        	 }			
	        };
			
			//create child directory of worker, assign id to that worker
			Workerpath = zkc.getZooKeeper().create("/worker/worker-", null, ZkConnector.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
			String[] temp = Workerpath.split("-");
			Workerid = temp[1];			//create workerid of this worker
			zkc.getChildren(JOBS_PATH+"/worker-"+Workerid, WorkerWatcher );
			
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
		
       
	}
	
	public String addToFreeWorker(WorkerObject wk){
		try{
			Process p = Runtime.getRuntime().exec("getconf _NPROCESSORS_ONLN");
    			p.waitFor();		//create shell object and retrieve cpucore number
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			wk.cpucore = br.readLine();
			wk.executionTime= this.executionTime;
			
			
			return wk.toNodeDataString();
		
		}catch (Exception e) {
            e.printStackTrace();
            return null;
        }
		
		
	}
	
	
	private String Create_WorkerObj(String wkid){
			String wkname= "worker-"+wkid;
			WorkerObject wkObject= new WorkerObject(wkname);
			return addToFreeWorker(wkObject);
		
	}
	
	
	
	private long benchmark(){
		long startTime = System.nanoTime();
		try{ Thread.sleep(1000); } catch (Exception e) {}
		long elapsedTime = System.nanoTime() - startTime;
		return elapsedTime;
	}
	
	
}
