/*
 * Responsibility for Worker:
 * 1.Run Benchmark of current available node, get speed information for Scheduler
 * 2.Get Hardware information and Benchmark information of the current node
 * 3.Create one instance
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
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException, NumberFormatException, ClassNotFoundException {
		if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:zkPort WorkerPort");
            return;
        }
		List<String> jobs = null, tasks = null; 
		Worker worker = new Worker(args[0]);

		System.out.println("Sleeping...");
		while (true) {
		    try{ Thread.sleep(5000); } catch (Exception e) {}
		} 
	 }
	
	public Worker(String hosts){
		benchmarkTime=benchmark();
		Hardware_config();
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
	                            if (path.equals(Workerpath)){
	                            	 	try{ Thread.sleep(1000); } catch (Exception e) {}	//assume this is running the child node for now
	                            		//now need to delete the directory
	                            		zkc.delete(Workerpath,-1);
	                            		//assume now updating result to RESULT_PATH directory
	                            		Create_WorkerObj();
	                            		
	                            }
	                            		

	                        } catch (Exception e) {
	                            e.printStackTrace();
	                        }
	                 	//case NodeDataChanged:
	                 		//;
	                 		
	                 }
	        	 }			
	        };
			
			//create child directory of worker, assign id to that worker
			Workerpath = zkc.getZooKeeper().create("/worker/worker-", null, ZkConnector.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
			String[] temp = Workerpath.split("-");
			Workerid = temp[1];			//create workerid of this worker
			zkc.getData(Workerpath, WorkerWatcher, null );
			
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
		
       
	}
	
	
	private void Create_WorkerObj(){
		String wkname= "worker-"+Workerid;
		WorkerObject NewObject= new WorkerObject(wkname);
		NewObject.benchmarkTime= benchmarkTime;
		
		
	}
	
	/*private execute{
		  	p = Runtime.getRuntime().exec("host -t a " + domain);
		    p.waitFor();
		 
		    BufferedReader reader = 
		         new BufferedReader(new InputStreamReader(p.getInputStream()));
		 
		    String line = "";			
		    while ((line = reader.readLine())!= null) {
			sb.append(line + "\n");
		    }
	}*/
	
	public void Hardware_config(){
		/* Total number of processors or cores available to the JVM */
	    //System.out.println("Available processors (cores): " + 
	        this.core=Runtime.getRuntime().availableProcessors();

	    /* Total amount of free memory available to the JVM */
	    //System.out.println("Free memory (bytes): " + 
	        this.mem_total_JVM=Runtime.getRuntime().freeMemory();

	    /* This will return Long.MAX_VALUE if there is no preset limit */
	        this.mem_max = Runtime.getRuntime().maxMemory();
	    /* Maximum amount of memory the JVM will attempt to use */
	    //System.out.println("Maximum memory (bytes): " + 
	      //  (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

	    /* Total memory currently available to the JVM */
	   // System.out.println("Total memory available to JVM (bytes): " + 
	       this.mem_cur_JVM = Runtime.getRuntime().totalMemory();
	       
	       this.hardware_info[0]=String.valueOf(this.core);
	       this.hardware_info[1]=String.valueOf(this.mem_total_JVM);
	       this.hardware_info[2]=String.valueOf(this.mem_max);
	       this.hardware_info[0]=String.valueOf(this.mem_cur_JVM);
	    
	}
	
	
	private long benchmark(){
		long startTime = System.nanoTime();
		try{ Thread.sleep(1000); } catch (Exception e) {}
		long elapsedTime = System.nanoTime() - startTime;
		return elapsedTime;
	}
	
	
}