import java.net.*;
import java.io.*;
import java.util.*;

class ScheduleAlgo {
	// May also depend on current state of the workers as well ??
	public static Hashtable<String, Queue<JobObject>> scheduleJobs(List<WorkerObject> workersList, List<JobObject> jobsList) {

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

        public static Integer getMaxNumberJobs(String hostName){
                if (hostName.equals("M1"))
                    return 2;
                else if (hostName.equals("M2"))
                    return 3;
                else
                    return 1;
        };

        public static Integer getEFT(String hostName, Integer qValue){
                if (hostName.equals("M1"))
                    return 15;
                else if (hostName.equals("M2"))
                    return 20;
                else if (hostName.equals("M3"))
                    return 15;
                else
                    return Integer.MAX_VALUE;
        };

        //Testing main
        public static void main(String[] args) {
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


                scheduledJobs = scheduleJobs(workerList, jobList);

                for (String key: scheduledJobs.keySet()){
                    System.out.println("For machine " + key + ": ");
                    for (JobObject JO: scheduledJobs.get(key)){
                    System.out.println("Job: " + JO.jobId);
                    }
                }
                }
                catch (Exception e) {
			e.printStackTrace();
		}
        }
}
