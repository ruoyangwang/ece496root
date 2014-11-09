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
	static Hashtable<String, String> requests= new Hashtable();
	private static void getConn(){
		System.out.println("In getConn");
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

		System.out.println("Got from zookeeper -"+resultData);
		String[] temp = resultData.split(":");

		try{
		socket = new Socket(temp[0], Integer.parseInt(temp[1]));

		Sout = null;
		Sin = null;

		System.out.println("getConn Successful host:"+temp[0]+" port:"+temp[1]);

		}catch(Exception e)
		{
			System.out.println("Exception at getConn: "+resultData);
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
		String i = "search";
		String q = "q";
		String trim;
		String s;	 
		int re=0;
		while(true){
			System.out.println("Enter new job, \"search\" to search for job with Tracking ID, \"q\" to quit");

			s = in.nextLine();
			trim =s.trim();
			re=0;
			String Request=null;
			String Reply=null;
			if(i.equalsIgnoreCase(trim)){

				System.out.println("Enter Tracking ID");
				s = in.nextLine();
				trim =s.trim();
	
				Request = "seq:"+trim;


			}else if(q.equalsIgnoreCase(trim)){

				System.out.println("Quitting");
				// quit
				return;

			}else{
				String k = requests.get(trim);
				if(k==null)
				{
					re =1;
					Request = "req:"+trim;
				}else{
			
					Request = "seq:"+k;
				}
				
			}

			if(socket == null){

				getConn();

			}
			int retry =1;
			System.out.println("Request is "+Request);
			while(retry ==1){
				if(socket==null){
					try {
						Thread.sleep(1000);
					} catch (Exception ex) {}
					getConn();

				}				

				try {
					if(Sout==null){
						System.out.println("new Sout created");
						Sout = new ObjectOutputStream(socket.getOutputStream());
					}
 					Sout.writeObject(Request); 
 				

					if(Sin == null)
						System.out.println("new Sin created");
 						Sin = new ObjectInputStream(socket.getInputStream());

 					Reply = (String) Sin.readObject();
 				

 				}
 				catch(Exception e){
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
			
			if(re==1){
				String [] sp = Request.split(":"); 			
				String [] rp = Reply.split(":"); 			
				requests.put(sp[1],rp[1].trim());
			}

			if(Reply.equalsIgnoreCase("ERROR"))
				Reply = "Tracking ID not found";
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
