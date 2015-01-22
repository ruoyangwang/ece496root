import java.net.*;
import java.io.*;
import java.util.*;
import java.sql.*;

class ScheduleAlgo {
    private Connection conn = null;
    private Statement statement = null;
    private PreparedStatement pstmt = null;
    private ResultSet rs = null;

	// May also depend on current state of the workers as well ??
    public Hashtable<String, Queue<JobObject>> scheduleJobs(List<WorkerObject> workersList, List<JobObject> jobsList) throws Exception {
    
        if ( ! checkWorkerExistenceInDB(workersList) ){
            System.out.println("No worker in db");
            throw new Exception("Worker info doesn't exist");
        }

        Hashtable<String, Queue<JobObject>> scheduledJobs = new Hashtable<String, Queue<JobObject>>();
            Hashtable<String, Integer> workerAvaliableTime = new Hashtable<String, Integer>();
            List<JobObject> jobListCopy = new ArrayList<JobObject>();
        
        // create a copy of jobList
        // NOTE: we want to keep the reference to jo the same as the original list
        for(JobObject jo: jobsList) {
        	jobListCopy.add(jo);
        }
            
            for(WorkerObject wo: workersList) {
        	// initialize scheduledJobs and workerAvaliableTime
        	String hostName = wo.getHostName();
        	scheduledJobs.put(hostName, new LinkedList<JobObject>());
                    workerAvaliableTime.put(hostName, 0);
                    //add MaxJobNumber to workers
                    wo.MaxJobNumber = getMaxNumberJobs(wo.getHostName());
//                    System.out.println("name:"+wo.getHostName()+"Job # "+ wo.MaxJobNumber);
        }
        
        // FIFO
        //while (jobListCopy.size() > 0) {
        //	for(WorkerObject wo: workersList) {
            //                //***String workerName = wo.getNodeName();
        //		String hostName = wo.getHostName();
        //		// remove the first job from list				
        //		JobObject j = jobListCopy.remove(0);
        //		if (j != null) {
        //			//***Queue<JobObject> q = scheduledJobs.get(workerName);
        //			Queue<JobObject> q = scheduledJobs.get(hostName);
        //			q.add(j);
        //		} else {
        //			// list is empty
        //			break;
        //		}
        //	}
        //}
        
            //MCT
            while (jobListCopy.size() > 0){
                    int minTime = Integer.MAX_VALUE;
                    WorkerObject minWorker = new WorkerObject();
                    // find min worker total time
                    for (WorkerObject wo: workersList){
                            String hostName = wo.getHostName();
                            int EFT = getEFT(hostName, 100) + workerAvaliableTime.get(hostName);
//                            System.out.println("Worker="+wo.getHostName()+" EFT="+ getEFT(hostName,100));
                            if (EFT < minTime){
                                    minTime = EFT;
                                    minWorker = wo;
                            }
                    }
                    //schedule jobs onto min total worker using MaxJobNumber
                    for (int i = 0; i < minWorker.MaxJobNumber; i++){
                            if (jobListCopy.size() > 0){
                                    JobObject j = jobListCopy.remove(0);
                                    //update workerAvaliableTime
                                    workerAvaliableTime.put(minWorker.getHostName(), minTime);
                                    //add job to scheduledJobs
                                    Queue<JobObject> q = scheduledJobs.get(minWorker.getHostName());
        	                q.add(j);
                            }
                    }
            }
        
    return scheduledJobs;
    };

    //TODO: Merge with the above constructor
    public ScheduleAlgo(){
        System.out.println("connecting");
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:./jobScheduler.db");
            statement = conn.createStatement();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        System.out.println("DB connection established");
    }

    public boolean checkWorkerExistenceInDB(List<WorkerObject> workersList){
      for(WorkerObject worker : workersList){
        try
        {
            pstmt = conn.prepareStatement("select * from workers where name = ?");
            pstmt.setString(1, worker.getHostName());
            rs = pstmt.executeQuery();
            if(!rs.next()){
                return false;
            }
        }
        catch(SQLException e)
        {
          System.err.println(e.getMessage());
          return false;
        }
      }
      return true;
    }

    public Integer getMaxNumberJobs(String hostName){
        Integer maxNumberJobs = 0;

        try
        {
            pstmt = conn.prepareStatement("select * from workers where name = ?");
            pstmt.setString(1, hostName);
            rs = pstmt.executeQuery();
            maxNumberJobs = rs.getInt("maxNumberJobs");
        }
        catch(SQLException e)
        {
          System.err.println(e.getMessage());
        }
//        System.out.println("DB: mName:"+hostName+" job # -> " + maxNumberJobs);
        return maxNumberJobs;
    }

    public Integer getEFT(String hostName, Integer qValue){
        Integer minEFT = 0;
        Integer maxEFT = 0;

        try
        {
            pstmt = conn.prepareStatement("select * from workers where name = ?");
            pstmt.setString(1, hostName);
            rs = pstmt.executeQuery();
            minEFT = rs.getInt("minEFT");
            maxEFT = rs.getInt("maxEFT");
        }
        catch(SQLException e)
        {
            System.err.println(e.getMessage());
        }

        Integer EFT = 0;
//        if (qValue>100){
//            EFT = maxEFT;
//        } else {
//            //EFT increase linearly from Q values ranging from 1-100
//            EFT = ((maxEFT-minEFT)/100)*qValue + minEFT;
//        }
        EFT = (maxEFT + minEFT) / 2;
        return EFT;
    }
}
