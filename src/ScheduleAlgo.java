import java.util.*;

/**
 * Scheduling algotrithm used to assign jobs to workers.
 *
 * @author Jie (Jacky) Liu, jjiiee.liu@mail.utoronto.ca
 */
class ScheduleAlgo {

	/**
	 * Schedule jobs to workers given a list of jobs and workers.
	 * 
	 * NOTE: A list of jobs currenly executing by each worker can be provided. See Scheduler.java getCurrentWorkerJobs()
	 * 
	 * @param workersList - a list of workers connected to the system
	 * @param jobsList - a list of unstarted jobs
	 * @return - a hashtable of job queues for each worker keyed by worker names.
	 */
	public static Hashtable<String, Queue<JobObject>> scheduleJobs(List<WorkerObject> workersList, List<JobObject> jobsList) {
		Hashtable<String, Queue<JobObject>> scheduledJobs = new Hashtable<String, Queue<JobObject>>();

		// initialize scheduledJobs with worker names.		
		for(WorkerObject wo: workersList) {
			String workerName = wo.getNodeName();
			scheduledJobs.put(workerName, new LinkedList<JobObject>());
		}

		// For now do random assignment.
		// Every worker grabs a job from the list until it is empty.
		while (jobsList.size() > 0) {
			for(WorkerObject wo: workersList) {
				String workerName = wo.getNodeName();
			
				JobObject j = null;
				if (jobsList.size()>0) {
					try {	
						// remove the first job from list	
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
				} else {
					break;
				}
			}
		}
		return scheduledJobs;
	};
}
