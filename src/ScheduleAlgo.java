import java.net.*;
import java.io.*;
import java.util.*;

class ScheduleAlgo {
	// May also depend on current state of the workers as well ??
	public static Hashtable<String, Queue<JobObject>> scheduleJobs(List<WorkerObject> workersList, List<JobObject> jobsList) {

		Hashtable<String, Queue<JobObject>> scheduledJobs = new Hashtable<String, Queue<JobObject>>();
		List<JobObject> jobListCopy = new ArrayList<JobObject>();

		for(WorkerObject wo: workersList) {
			// initialize scheduledJobs
			String workerName = wo.getNodeName();
			scheduledJobs.put(workerName, new LinkedList<JobObject>());
		}


		// create a copy of jobList
		// NOTE: we want to keep the reference to jo the same as the original list
		for(JobObject jo: jobsList) {
			jobListCopy.add(jo);
		}

		// For now do random assign.
		// Every worker grabs a job from the list.
		while (jobListCopy.size() > 0) {
			for(WorkerObject wo: workersList) {
				String workerName = wo.getNodeName();
				// remove the first job from list				
				JobObject j = null;
				if (jobListCopy.size()>0) {
					try {	
						j=jobListCopy.remove(0);
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
