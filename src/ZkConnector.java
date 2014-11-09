import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.io.IOException;

public class ZkConnector implements Watcher {

    // ZooKeeper Object
    ZooKeeper zooKeeper;

    // To block any operation until ZooKeeper is connected. It's initialized
    // with count 1, that is, ZooKeeper connect state.
    CountDownLatch connectedSignal = new CountDownLatch(1);
    
    // ACL, set to Completely Open
    protected static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

    /**
     * Connects to ZooKeeper servers specified by hosts.
     */
    public void connect(String hosts) throws IOException, InterruptedException {

        zooKeeper = new ZooKeeper(
                hosts, // ZooKeeper service hosts
                5000,  // Session timeout in milliseconds
                this); // watcher - see process method for callbacks
	    connectedSignal.await();
    }

    /**
     * Closes connection with ZooKeeper
     */
    public void close() throws InterruptedException {
	    zooKeeper.close();
    }

    /**
     * @return the zooKeeper
     */
    public ZooKeeper getZooKeeper() {
        // Verify ZooKeeper's validity
        if (null == zooKeeper || !zooKeeper.getState().equals(States.CONNECTED)) {
	        throw new IllegalStateException ("ZooKeeper is not connected.");
        }
        return zooKeeper;
    }

    protected Stat exists(String path, Watcher watch) {
        
        Stat stat =null;
        try {
            stat = zooKeeper.exists(path, watch);
        } catch(Exception e) {

			System.out.println("Exception thrown at exists. watch not set");
        }
        
        return stat;
    }
    
    protected List<String> getChildren(String path, Watcher watcher) {
        
        List<String> childrenList = null;
        try {
        	childrenList = zooKeeper.getChildren(path, watcher);
        } catch(Exception e) {
        }
        
        return childrenList;
    }
    
    protected List<String> getChildren(String path) {
        
        List<String> childrenList = null;
        try {
        	childrenList = zooKeeper.getChildren(path, false);
        } catch(Exception e) {
        }
        
        return childrenList;
    }
    
    protected String getData(String path, Watcher watcher, Stat stat){
    	
    	byte[] byteData = null;
    	try {
			byteData = zooKeeper.getData(path, watcher, stat);
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(byteData != null)
			return new String(byteData);
		else
			return null;
    }

    protected KeeperException.Code create(String path, String data, CreateMode mode) {
        
        try {
            byte[] byteData = null;
            if(data != null) {
                byteData = data.getBytes();
            }
            zooKeeper.create(path, byteData, acl, mode);
            
        } catch(KeeperException e) {
            return e.code();
        } catch(Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }
        
        return KeeperException.Code.OK;
    }

    public void process(WatchedEvent event) {
        // release lock if ZooKeeper is connected.
        if (event.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }


 	protected KeeperException.Code delete(String path, int version){

		try{
			zooKeeper.delete(path, version);

        } catch(KeeperException e) {
            return e.code();
        } catch(Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }

        return KeeperException.Code.OK;
	}



    protected Stat setData(String path, String data, int version) {
        
        Stat stat =null;
        try {

 			byte[] byteData = null;
            if(data != null) {
                byteData = data.getBytes();
            }

            stat = zooKeeper.setData(path, byteData,version);
        } catch(Exception e) {
        }
        
        return stat;
    }


}

