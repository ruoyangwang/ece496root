/**
 * JobStatusObject represebt a job status.
 * Provides functions to parse data from/to znodes and
 * functions to set status, start time and end time.
 * 
 * The JobStatusObject znodes is current used in:
 * 	/result/{JobStatusObject}
 *
 * @author Jie (Jacky) Liu, jjiiee.liu@mail.utoronto.ca
 */
public class JobStatusObject {
	public Integer totalJobs;
	public String status;
	public Long startTime; // milli seconds 
	public Long endTime; // milli seconds 
	public Long executionTimeM; // in mins
	public Long executionTimeH; // in hours
	
	// Status constants
	public final String COMPLETED = "COMPLETED";
	public final String KILLED = "KILLED";
	public final String ACTIVE = "ACTIVE";

	public final String DIVIDER = ":";

	/**
 	 * Constructures for JobStatusObject
	 */
	public JobStatusObject() {
		this.totalJobs = null;
		this.status = null;
		this.startTime = null;
		this.endTime = null;
	}

	public JobStatusObject(Integer totalJobs) {
		this.totalJobs = totalJobs;
		this.status = ACTIVE;
		this.startTime = null;
		this.endTime = null;
	}

	public JobStatusObject(String totalJobs) {
		this.totalJobs = Integer.parseInt(totalJobs);
		this.status = ACTIVE;
		this.startTime = null;
		this.endTime = null;
	}

	/**
	 * Set status to killed.
	 */
	public void killed() {
		this.status = KILLED;
	}

	
	/**
	 * Set status to completed and the end time.
	 * Calculates the execution time.
	 */
	public void completed() {
		this.status = COMPLETED;
		this.endTime = System.currentTimeMillis();
		this.getExecutionTime();
	}


	/**
	 * Calculates the execution time based on the start and end time.
	 * @return - execution time in minutes
	 */
	public Long getExecutionTime() {
		if (endTime != null && startTime != null) {
			this.executionTimeM = (endTime - startTime) / 60000L;
			this.executionTimeH = this.executionTimeM / 60L;
		}

		return executionTimeM;
	}

	/**
	 * Parse znode data to JobStatusObject
	 * @param znodeData - znode data for a JobStatusObject
	 */
	public void toObj(String znodeData) {
		if (znodeData != null) {
			String[] data = znodeData.split(DIVIDER);
			switch (data.length) {
				case 6:
					this.executionTimeM = Long.valueOf(data[4]);
					this.executionTimeH = Long.valueOf(data[5]);
				case 4:
					this.endTime = Long.valueOf(data[3]);
				case 3:
					this.startTime = Long.valueOf(data[2]);
				case 2:
					this.status = data[1].toUpperCase();
					this.totalJobs = Integer.valueOf(data[0]);
					break;
				default:
					System.out.println("error in converting znode to job status obj");
					break;
			}
		}
	}

	/**
	 * Get the string representation of JobStatusObject to store in znode.
	 */
	public String toZnode() {
		if (totalJobs == null || status == null) {
			System.out.println("error in converting job status obj to znode");
			return null;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(totalJobs.toString());
		sb.append(DIVIDER);
		sb.append(status.toString());

		if (startTime != null) {
			sb.append(DIVIDER);
			sb.append(startTime.toString());
			if (endTime != null) {
				sb.append(DIVIDER);
				sb.append(endTime.toString());	
				if (executionTimeM != null && executionTimeH != null) {
					sb.append(DIVIDER);
					sb.append(executionTimeM);
					sb.append(DIVIDER);
					sb.append(executionTimeH);
				}
				
			}
		}
		return sb.toString();
	}
}
