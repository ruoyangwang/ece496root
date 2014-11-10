import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	ZkConnector zkc;
	String Wokerid;
	
	public Worker(String hosts){
		zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
            
			Stat stats = zkc.exists("/worker",null);
			if(stats==null) {						//create repository
				zkc.getZooKeeper().create("/worker", null, ZkConnector.acl, CreateMode.PERSISTENT);
				System.out.println("/worker created");
			}
			
			String path = zkc.getZooKeeper().create("/worker/worker-", null, ZkConnector.acl, CreateMode.EPHEMERAL_SEQUENTIAL);
			String[] temp = path.split("-");
			Workerid = temp[1];			//get workerid of this worker
			
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
		
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
		;
	}
	
	
	
}