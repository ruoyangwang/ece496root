import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
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


public class Worker {
	
	ZkConnector zkc;
	
    Watcher fsWatcher;
    Watcher jobsWatcher;
    Watcher resultWatcher;
    
    String hashPasswd; // Hashed password to compare
    String jobID = "";
    String partitionID;
	String fsPath = "/fileserver";
	String jpPath = "/jobpool";
	String workerID;
	
	boolean conn = false;
	boolean abortTask = false;
	
	private Socket socket;
	private ObjectOutputStream out = null;
    private ObjectInputStream in = null;
	private static String hostname;
	private static int port;

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException {
		
		if (args.length != 1) {
		    System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
		    return;
		}

		Worker w = new Worker(args[0]);
		
		w.checkFsPath();
	//	w.checkJpPath();
		
		System.out.println("Sleeping...");
		while (true) {
		    try{ Thread.sleep(5000); } catch (Exception e) {}
		}   

	}
	
	
	public Worker(String hosts) {
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
            
			Stat stats = zkc.exists("/worker",null);
			if(stats==null) {
				zkc.getZooKeeper().create("/worker", null, ZkConnector.acl, CreateMode.PERSISTENT);
				System.out.println("/worker created");
			}


            String path = zkc.getZooKeeper().create("/worker/worker-", null, ZkConnector.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
            String[] temp = path.split("-");
            workerID = temp[1];
            System.out.println(path+" created");

			

        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
 
        // Create a new worker znode.
      /*  try {
			zkc.getZooKeeper().create("/worker/worker-", null, ZkConnector.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("/worker/worker");
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
        
        fsWatcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };
                            
	    jobsWatcher = new Watcher() { // Anonymous Watcher
					        @Override
					        public void process(WatchedEvent event) {
					            handleEvent(event);
					    
					        } };
					        
        resultWatcher = new Watcher() { // Anonymous Watcher
					        @Override
					        public void process(WatchedEvent event) {
					            handleEvent(event);
					    
					        } };
                            
    }
    
    private void checkFsPath() {
    	 System.out.println("in checkFsPath");
    	Stat stat = null;
    	stat = zkc.exists(fsPath, fsWatcher);
    	if(stat == null){
			 System.out.println(fsPath+" is null. returning");
    		return;
    	}
    		
    	String nodeData = zkc.getData(fsPath, fsWatcher, stat);
        if(nodeData == null){
			System.out.println(fsPath+" cannot getData");
		}
        //System.out.println("after getData from fsPath: " + nodeData);
        if(conn){

				System.out.println("conn already setup");
		}
        if (nodeData != null && !conn) {
            System.out.println("Connecting to " + nodeData);
            
            String[] temp = nodeData.split(":");
            hostname = temp[0];
            port = Integer.parseInt(temp[1]);
            
            // Connect to primary file server.
            try {
        		socket = new Socket(hostname, port);
            } catch (IOException e) {
                System.err.println("ERROR: Could not listen on port!");
                System.exit(-1);
            }
            
			try {
				out = new ObjectOutputStream(socket.getOutputStream());
				in = new ObjectInputStream(socket.getInputStream());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			conn = true;
			// Ready to accept job.
			// Try to get a job.
			checkJpPath();
			
		//	System.exit(0);
			
						

            
        }else if (conn){

			checkJpPath();
		}
        
    }
    
    private void checkJpPath() {
    	
    	 System.out.println("in checkJpPath");
    	// Abort current task.
    	if(abortTask){
    		System.out.println("Abort task: /jobs/worker-"+workerID+"/"+jobID+"-"+partitionID);
    		Stat stat11 = zkc.exists("/jobs/worker-"+workerID+"/"+jobID+"-"+partitionID, null);
			if(stat11 != null)
				zkc.delete("/jobs/worker-"+workerID+"/"+jobID+"-"+partitionID);
    		hashPasswd = "";
    	    jobID = "";
    	    partitionID = "";
    	}
    	
    	// Get list of job IDs.
    	List<String> jobList;
    	jobList = zkc.getChildren("/jobpool"); // Don't watch.
	    if(jobList==null || jobList.size() == 0){
	    	jobList = zkc.getChildren("/jobpool", jobsWatcher); // re-enable watch
	    	System.out.println("Watch on " + "/jobpool" + " is set.");
	    	

	    	return;
	    	
			//while (true) {
			//    try{ System.out.println("Sleeping...");
 		//		Thread.sleep(5000); } catch (Exception e) {}
		//	}   
	    }
	    	
	    else {
	    	System.out.println("Finding job... Size of jobs: " + jobList.size());
	    	
	    	// Get earliest job.
	    	Collections.sort(jobList);
	    	System.out.println(jobList.get(0));
	    	
	    	String jIDPath = "/jobpool/" + jobList.get(0);
	    	

			// Delete its other jobs if exist
			List<String> lala = zkc.getChildren("/jobs/worker-"+workerID);
			if(lala!=null && lala.size()!=0){
	    		zkc.delete("/jobs/worker-"+workerID+"/"+lala.get(0));
			}
	    	// assume /jobpool/12/id-partition
	    	//jobID = jIDPath.substring(12, jIDPath.length());
	    	
	    	System.out.println("result watcher on: /result/"+jobList.get(0));
	    	
	    	// need to watch the result of the jobID
	    	List<String> stat;
	    	stat = zkc.getChildren("/result/"+jobList.get(0), resultWatcher);
	    	if(stat == null){
	    		System.err.println("results watch cannot be set: "+"/result/"+jobList.get(0));
	    		return;
	    	}
	    	
	    	System.out.println("size of result: " + stat.size());
	    	//System.out.println("jobID: " + jobID + " jIDPath: " + jIDPath);
	    	System.out.println("jIDPath: " + jIDPath);
	    	jobList = zkc.getChildren(jIDPath);
	    	System.out.println("size of children: " + jobList.size());
	    	
	    	String[] temp = jobList.get(0).split("-");
	    	jobID = temp[0];
	    	partitionID = temp[1];
	    	System.out.println("jobID: " + jobID + " jIDPath: " + jIDPath);
	    	
	    	String taskPath = jIDPath + "/" + jobList.get(0);
	    	
	    	System.out.println("taskPath: " + taskPath);
	    	
			Stat stat01 = zkc.exists(taskPath, null);
			if(stat01 != null)
	    		hashPasswd = zkc.getData(taskPath, null, null);
			else {
				checkJpPath();
				return;
			}
	    	System.out.println("hashPasswd: " + hashPasswd);
	    	
	    	// add the task to itself.
	    	try {
	    		System.out.println("Add task to itself: /jobs/worker-"+workerID+"/"+jobID+"-"+partitionID);
				zkc.getZooKeeper().create("/jobs/worker-"+workerID+"/"+jobID+"-"+partitionID, hashPasswd.getBytes(), ZkConnector.acl, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    		    	
	    	// delete it from job pool.
			Stat stat11 = zkc.exists(taskPath, null);
			if(stat11 != null)
				zkc.delete(taskPath);
	    	
	    	// delete the task if has only last partition to check.
			stat11 = zkc.exists(jIDPath, null);
			jobList = zkc.getChildren(jIDPath);
	    	if(jobList.size()==0 && stat11!=null){
	    		
				zkc.delete(jIDPath);
	    	}

//try{ Thread.sleep(5000); } catch (Exception e) {}

	    	
	    	// Set jobID and partitionID
	    	// assume /jobpool/job12/1
	    	//partitionID = taskPath.substring(jIDPath.length()+1, taskPath.length());
	    	
	    	//System.out.println("jobID: " + Integer.parseInt(jobID) + " partitionID: " + Integer.parseInt(partitionID));
	    	
	    	if(conn){
				FSPacket packetToServer = new FSPacket();
				
				String passwdMatched = null;
				
				// Partition ID is set based on the task...
				//System.out.println("in conn: " + partitionID);
				packetToServer.partition = Integer.parseInt(partitionID);
				//System.out.println("in conn: " + partitionID);
				
				try {
					out.writeObject(packetToServer);
					FSPacket packetFromServer = (FSPacket)in.readObject();
				
					//System.out.println("1. " + packetFromServer.dictWords[0]);
					//System.out.println("7817. " + packetFromServer.dictWords[7816]);
					
					// Compare the password hash with the words from the dictionary.
					for(int i=0; i<FSPacket.PARTITION_SIZE; i++){
						// password hash. Hard code it for now.
						//hashPasswd = getHash("ad0re");
						
						if(hashPasswd.equals(getHash(packetFromServer.dictWords[i]))){
							// Match found. Send results.
							System.out.println("Passwd match found! Pasword is " + packetFromServer.dictWords[i]);
							passwdMatched = packetFromServer.dictWords[i];
							break;
							// Return and watch for new task.
							// Not yet write... Need to watch jobpool.
						}
					}
					
					// Sent the result.
					stat = zkc.getChildren("/result/" + jobID);
					if(stat == null){
						System.err.println("Result cannot be sent. Path not created for this job ID.");
						return;
					}

					// Delete the task from itself. Task complete.
					Stat stat122 = zkc.exists("/worker/worker-"+workerID+"/"+jobID+"-"+partitionID, null);
					if(stat122 != null){

						zkc.delete("/worker/worker-"+workerID+"/"+jobID+"-"+partitionID);
						System.out.println("Deleted from itself: /worker/worker-"+workerID+"/"+jobID+"-"+partitionID);
					}
					
					// Password matched.
					if(passwdMatched != null){
						System.out.println("/result/"+jobID+"/"+Integer.parseInt(partitionID));
						
						// Update the result for the job.
						zkc.getZooKeeper().setData("/result/"+jobID, passwdMatched.getBytes(), -1);
						
						// Delete other sub-results.
						List<String> childrenList = zkc.getChildren("/result/"+jobID);
						for(int i=0; i<childrenList.size(); i++){
							Stat stat111 = zkc.exists("/result/"+jobID+"/"+Integer.parseInt(childrenList.get(i)), null);
							if(stat111 != null)
								zkc.delete("/result/"+jobID+"/"+Integer.parseInt(childrenList.get(i)));
						}
						
						// Delete all tasks related to this job ID.
						Stat stat111 = zkc.exists("/jobpool/"+jobID, null);
						if(stat111 != null)
							zkc.delete("/jobpool/"+jobID);
					}
					else {
						Stat stat1 = zkc.exists("/result/"+jobID+"/"+Integer.parseInt(partitionID), null);
						if(stat1 == null)
							zkc.getZooKeeper().create("/result/"+jobID+"/"+Integer.parseInt(partitionID), 
									"fail...".getBytes(), ZkConnector.acl, CreateMode.PERSISTENT);
							
						// See if there are already 34 results.
						List<String> childrenList = zkc.getChildren("/result/" + jobID);
						
						// Task Complete. 
						if(childrenList.size()==34){
							System.out.println("34 results.");
							
							// Update the result for the job.
							zkc.getZooKeeper().setData("/result/"+jobID, "failed...".getBytes(), -1);
							
							// Delete other sub-results.
							for(int i=0; i<34; i++){
								Stat stat111 = zkc.exists("/result/"+jobID+"/"+i, null);
								if(stat111 != null)
									zkc.delete("/result/"+jobID+"/"+i);
							}
							
							// Delete all tasks related to this job ID.
							Stat stat111 = zkc.exists("/jobpool/"+jobID, null);
							if(stat111 != null)
								zkc.delete("/jobpool/"+jobID);
						}
					}
					
					stat = zkc.getChildren("/result/" + jobID);
					if(stat != null){
						System.out.println("Size of results after adding: " + stat.size());
					}
					
					System.out.println("Try to get another job.");
					checkJpPath();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			}
			
			try {
				out.close();
				in.close();
				socket.close();
				conn = false;
			//	System.exit(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	    	
	    	
	    }
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(fsPath)) {
            if (type == EventType.NodeDeleted) {
//                System.out.println(fsPath + " deleted! Let's reconnect!");  
            	conn = false;
                checkFsPath(); // try to reconnect to the new primary fs
            }
            if (type == EventType.NodeCreated) {
                System.out.println(fsPath + " created!");       
 //               try{ Thread.sleep(5000); } catch (Exception e) {}
                checkFsPath(); // re-enable the watch
            }
        }
        
        else if(path.equalsIgnoreCase(jpPath)) {
//            if (type == EventType.NodeDeleted) {
//                System.out.println(fsPath + " deleted! Let's reconnect!");       
//                checkpath(); // try to become the boss
//            }
//            if (type == EventType.NodeCreated) {
//                System.out.println(fsPath + " created!");       
// //               try{ Thread.sleep(5000); } catch (Exception e) {}
//                checkpath(); // re-enable the watch
//            }
System.out.println("In jobpool watcher: "+type);

            if (type == EventType.NodeChildrenChanged) {
                System.out.println("Children in " + jpPath + " changed!");  
try{ Thread.sleep(1000); } catch (Exception e) {}     
                checkFsPath(); // re-enable the watch
            }
        }
        
        else if(path.equalsIgnoreCase("/result/"+jobID)) {
//          if (type == EventType.NodeDeleted) {
//              System.out.println(fsPath + " deleted! Let's reconnect!");       
//              checkpath(); // try to become the boss
//          }
//          if (type == EventType.NodeCreated) {
//              System.out.println(fsPath + " created!");       
////               try{ Thread.sleep(5000); } catch (Exception e) {}
//              checkpath(); // re-enable the watch
//          }


          if (type == EventType.NodeDataChanged) {
              System.out.println("Node data changed in " + "/result/" + jobID);
              abortTask = true;
              checkJpPath(); // re-enable the watch
          }
      }
    }
    
    public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }
    

}
