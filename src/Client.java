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


public class Client {

	static Socket socket;
    static private ZkConnector zkc;
    static ObjectInputStream Sin = null;
    static ObjectOutputStream Sout = null;

	private static void getConn(){
		String resultData;
		Stat stat = null;
		String rPath = "/jobTracker";
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

		//System.out.println("Got from zookeeper -"+resultData);
		String[] temp = resultData.split(":");

		try{
			socket = new Socket(temp[0], Integer.parseInt(temp[1]));

			Sout = null;
			Sin = null;

			//System.out.println("getConn Successful host:"+temp[0]+" port:"+temp[1]);

		}catch(Exception e)
		{
			//System.out.println("Exception at getConn: "+resultData);
		}


	}

	public static void main(String[] args) {


		if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Client zkServer:clientPort");
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
			System.out.println("");
			System.out.println("Enter:");
			System.out.println("\"run\" followed by an input file, Q values and hosts to start a new job");
			System.out.println("ex: \"run inputfile.txt 1-3,5 c123,c124,c125\" would run npairs with inputfile,txt with Q values 1,2,3,5 on machiens c123 c124 and c125");
			System.out.println("\"add\" followed by hosts to add more machies to the computation");
			System.out.println("\"status\" follow by tracking ID to get status for the job");
			System.out.println("\"q\" to quit");
			System.out.println("");

			String s = in.nextLine();
			String userCommand = s.trim();

			String Request=null;
			String Reply=null;

			List<String> commandComponents = Arrays.asList(userCommand.split(" "));
			String type = commandComponents.get(0);

			if(run.equalsIgnoreCase(type)){
			
				String inputFile = null;
				String qValues = null;
				String hosts = null;
					
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
	
				Request = "run:" + inputFile + ":" + qValues + ":" +hosts;

			}else if (status.equalsIgnoreCase(type)) {
				String jobId = null;
					
				if (commandComponents.size() >= 2) {
					jobId = commandComponents.get(1);
				}

				if (jobId == null) {
					System.out.println("Enter Job ID");
					jobId = in.nextLine().trim();
				}
	
				Request = "status:" + jobId;

			}else if (kill.equalsIgnoreCase(type)) {
				String jobId = null;
					
				if (commandComponents.size() >= 2) {
					jobId = commandComponents.get(1);
				}

				if (jobId == null) {
					System.out.println("Enter Job ID");
					jobId = in.nextLine().trim();
				}
	
				Request = "kill:" + jobId;

			}else if(q.equalsIgnoreCase(type)){

				System.out.println("Quitting");
				// quit
				return;

			}else if (q.equalsIgnoreCase(addHost)) {
				// addHost

				String hosts = null;
				if (commandComponents.size() == 2) {
					hosts = commandComponents.get(1);				
				}

				if (hosts == null) {
					System.out.println("Enter hosts(comma separated)");
					hosts = in.nextLine().trim();
				}
				Request = "add:" + hosts;
			} else {
				System.out.println("Unknown request");
				continue;
			}

			if(socket == null){
				getConn();
			}

			int retry =1;
			System.out.println("Request is "+Request);
			while(retry == 1){
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
					continue;
 				}
				retry =0;
			} 
		
			System.out.println(Reply);
			try{
				Sin.close();
				Sout.close();
				socket.close();
				Sin = null;
				Sout = null;
				socket = null;
			}catch (Exception ex){
				Sin = null;
				Sout = null;
				socket = null;
			}	
		}


	}


}
