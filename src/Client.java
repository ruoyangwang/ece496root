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
 * Responsibilities of Client:
 * 1. Submit appropriatly formatted requests to jobtracker.
 *
 * @author Jie (Jacky) Liu, jjiiee.liu@mail.utoronto.ca
 */
public class Client {

	static Socket socket;
    static private ZkConnector zkc;
    static ObjectInputStream Sin = null;
    static ObjectOutputStream Sout = null;

	final static boolean DEBUG = true;

	/**
	 * Connection to jobtracker with jobtracker location information from zookeeper.
	 */
	private static void getConn(){
		String resultData;
		Stat stat = null;
		String rPath = "/jobTracker";

		// Get jobtracker information
		stat = zkc.exists(rPath, null);
		if(stat == null){
			System.out.println("Error: jobTracker does not exist. Try again later.");
			return;
		}

		resultData = zkc.getData(rPath, null, stat);
		if(resultData == null){
			System.out.println("Error: failed to get data from jobTracker. Try again later.");
			return;
		}

		// Connect to jobtracker
		String[] jobTrackerLocation = resultData.split(":");
		try {
			socket = new Socket(jobTrackerLocation[0], Integer.parseInt(jobTrackerLocation[1]));
			Sout = null;
			Sin = null;
			if (DEBUG)
				System.out.println("getConn Successful host:"+jobTrackerLocation[0]+" port:"+jobTrackerLocation[1]);

		} catch(Exception e) {
			if (DEBUG)
				System.out.println("Exception at getConn: "+resultData);
		}
	}

	/**
	 * Take user inputs and submit requests to jobtracker
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
            System.out.println("Usage: sh ./bin/client.sh zkServer");
            return;
        }

		socket= null;
 		zkc = new ZkConnector();

        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

		
		Scanner in = new Scanner(System.in);
		String q = "q";
		String run = "run";
		String status = "status";
		String addHost = "add";
		String kill = "kill";

		while(true){
			// help text
			System.out.println("");
			System.out.println("Enter:");
			System.out.println("\"run\" followed by an input file, Q values and hosts to start a new job");
			System.out.println("ex: \"run inputfile 1-3,5 c123,c124,c125\" would run npairs to execute with inputfile and Q values 1,2,3,5 on machiens c123 c124 and c125");
			System.out.println("\"add\" followed by hosts to add more machies to the computation");
			System.out.println("\"status\" follow by tracking ID to get status for the job");
			System.out.println("\"kill\" follow by tracking ID to get kill the job");
			System.out.println("\"q\" to quit");
			System.out.println("");

			String s = in.nextLine();
			String userCommand = s.trim();

			String Request=null;
			String Reply=null;

			List<String> commandComponents = Arrays.asList(userCommand.split(" "));
			String type = commandComponents.get(0);

			// run (new job) request
			if(run.equalsIgnoreCase(type)){
				String inputFile = null;
				String qValues = null;
				String hosts = null;
					
				// make sure we have all params
				if (commandComponents.size() >= 4) {
					inputFile = commandComponents.get(1);
					qValues = commandComponents.get(2);
					hosts = commandComponents.get(3);
				} else if (commandComponents.size() == 3) {
					inputFile = commandComponents.get(1);
					qValues = commandComponents.get(2);
				} else if (commandComponents.size() == 2) {
					inputFile = commandComponents.get(1);				
				}

				if (inputFile == null) {
					System.out.println("Enter input file");
					inputFile = in.nextLine().trim();
				}

				if (qValues == null) {
					System.out.println("Enter Q values");
					qValues = in.nextLine().trim();
				}

				if (hosts == null) {
					System.out.println("Enter hosts(comma separated)");
					hosts = in.nextLine().trim();
				}
	
				// format request.
				Request = "run:" + inputFile + ":" + qValues + ":" +hosts;
			
			// status request
			}else if (status.equalsIgnoreCase(type)) {
				String jobId = null;
				// make sure we have all params
				if (commandComponents.size() >= 2) {
					jobId = commandComponents.get(1);
				}

				if (jobId == null) {
					System.out.println("Enter Job ID");
					jobId = in.nextLine().trim();
				}
	
				Request = "status:" + jobId;

			// kill request
			}else if (kill.equalsIgnoreCase(type)) {
				String jobId = null;
				// make sure we have all params
				if (commandComponents.size() >= 2) {
					jobId = commandComponents.get(1);
				}

				if (jobId == null) {
					System.out.println("Enter Job ID");
					jobId = in.nextLine().trim();
				}
	
				Request = "kill:" + jobId;

			// add worker request
			} else if (addHost.equalsIgnoreCase(type)) {
				String hosts = null;
				// make sure we have all params
				if (commandComponents.size() == 2) {
					hosts = commandComponents.get(1);				
				}

				if (hosts == null) {
					System.out.println("Enter hosts(comma separated)");
					hosts = in.nextLine().trim();
				}
				Request = "add:" + hosts;

			// quitting client
			} else if(q.equalsIgnoreCase(type)){

				System.out.println("Quitting");
				// quit
				return;

			} else {
				System.out.println("Unknown request");
				continue;
			}

			if(socket == null){
				getConn();
			}

			int retry =1;
			if (DEBUG)
				System.out.println("Request is "+Request);

			// Submit the fomatted request to jobtracker
			while(retry == 1){
				// try to get connection if we dotn have a socket.
				if (socket == null) {
					try {
						Thread.sleep(1000);
					} catch (Exception ex) {}
					getConn();
				}				

				try {
					if(Sout==null){
						//System.out.println("new Sout created");
						Sout = new ObjectOutputStream(socket.getOutputStream());
					}

 					Sout.writeObject(Request); 
 				

					if(Sin == null)
						//System.out.println("new Sin created");
 						Sin = new ObjectInputStream(socket.getInputStream());

 					Reply = (String) Sin.readObject();

 				} catch (Exception e) {
					System.out.println("Exception at writeObject/readObject");
					e.printStackTrace();

					try {
						Thread.sleep(5000);
					} catch (Exception ex) {}

					getConn();
					// retry
					continue;
 				}
				retry =0;
			} 
		
			System.out.println(Reply);
			try {
				Sin.close();
				Sout.close();
				socket.close();
				Sin = null;
				Sout = null;
				socket = null;
			} catch (Exception ex) {
				Sin = null;
				Sout = null;
				socket = null;
			}	
		}
	}
}
