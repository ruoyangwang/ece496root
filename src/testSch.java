import java.sql.*;
import java.util.*;

class testSch{
    public static void main(String[] args){
        ScheduleAlgo sch = new ScheduleAlgo();
        System.out.println("test scheduler database");

        System.out.println("-------test maxNumberJobs-------");
        Integer maxJobs = 0;
        try{
            maxJobs = sch.getMaxNumberJobs("testnode1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (maxJobs == 2){
            System.out.println("pass");
        } else {
            System.out.println("Fail");
        }

        System.out.println("-------test EFT-------");
        Integer EFT = sch.getEFT("testnode1", 50);
        if (EFT == 50){
            System.out.println("pass");
        } else {
            System.out.println("Fail");
        }

        System.out.println("-------test check workers-------");
        List<WorkerObject> workers = new ArrayList<WorkerObject>();
        WorkerObject worker1 = new WorkerObject();
        WorkerObject worker2 = new WorkerObject();
        WorkerObject worker3 = new WorkerObject();
        worker1.setHostName("testnode1");
        worker2.setHostName("testnode2");
        worker3.setHostName("notExist");
        workers.add(worker1);
        workers.add(worker2);
        if (sch.checkWorkerExistenceInDB(workers)){
            System.out.println("pass");
        } else {
            System.out.println("Fail");
        }

        workers.add(worker3);
        if (!sch.checkWorkerExistenceInDB(workers)){
            System.out.println("pass");
        } else {
            System.out.println("Fail");
        }


        try{
            List<WorkerObject> workerList = new ArrayList<WorkerObject>();
            List<JobObject> jobList = new ArrayList<JobObject>();
            Hashtable<String, Queue<JobObject>> scheduledJobs = new Hashtable<String, Queue<JobObject>>();
            WorkerObject w1,w2,w3;
            JobObject j1,j2,j3,j4,j5;

            //define worker
            w1 = new WorkerObject();
            w1.setHostName("M1");
            w2 = new WorkerObject();
            w2.setHostName("M2");
            w3 = new WorkerObject();
            w3.setHostName("M3");

            //define jobs
            j1 = new JobObject(1, 188);
            j2 = new JobObject(2, 124);
            j3 = new JobObject(3, 158);
            j4 = new JobObject(4, 158);
            j5 = new JobObject(5, 158);

            workerList.add(w1);
            workerList.add(w2);
            workerList.add(w3);
            jobList.add(j1);
            jobList.add(j2);
            jobList.add(j3);
            jobList.add(j4);
            jobList.add(j5);


            scheduledJobs = sch.scheduleJobs(workerList, jobList);

            for (String key: scheduledJobs.keySet()){
                System.out.println("For machine " + key + ": ");
                for (JobObject JO: scheduledJobs.get(key)){
                System.out.println("Job: " + JO.jobId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        


//        Connection conn = null;
//        Statement statement = null;
//
//        System.out.println("connecting");
//        try {
//            Class.forName("org.sqlite.JDBC");
//            conn = DriverManager.getConnection("jdbc:sqlite:/Users/lizwang/Development/npairs/zookeeper/src/jobScheduler.db");
//            statement = conn.createStatement();
//        } catch ( Exception e ) {
//            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
//            System.exit(0);
//        }
//        System.out.println("Got conn");
    }
}
