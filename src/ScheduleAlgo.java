import java.net.*;
import java.io.*;
import java.util.*;

class ScheduleAlgo {
	// May also depend on current state of the workers as well ??
	public static Hashtable<String, Queue<JobObject>> scheduleJobs(List<WorkerObject> workersList, List<JobObject> jobsList, Hashtable<String, List<JobObject>> currentWorkerJobs) {

		Hashtable<String, Queue<JobObject>> scheduledJobs = new Hashtable<String, Queue<JobObject>>();
		
		for(WorkerObject wo: workersList) {
			// initialize scheduledJobs
			String workerName = wo.getNodeName();
			scheduledJobs.put(workerName, new LinkedList<JobObject>());
		}


		// For now do random assign.
		// Every worker grabs a job from the list.
		while (jobsList.size() > 0) {
			for(WorkerObject wo: workersList) {
				String workerName = wo.getNodeName();
				// remove the first job from list				
				JobObject j = null;
				if (jobsList.size()>0) {
					try {	
						j=jobsList.remove(0);
					} catch (Exception e) {
						j=null;
					}
 				} else {
					// list empty
					break;
				}
				if (j != null) {
					Queue<JobObject> q = scheduledJobs.get(workerName);
					q.add(j);
				}
			}
		}

		return scheduledJobs;
	};
}
