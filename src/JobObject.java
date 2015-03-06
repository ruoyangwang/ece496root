/**
 * JobObject represents a job.
 * Provides functions to parse data from/to znodes.
 *
 * The JobObject znodes is current used in:
 * 	/jobpool/{jobId}/{JobObject} 
 * and
 * 	/jobs/{workerName}/{JobObject} 
 *
 * @author Jie (Jacky) Liu, jjiiee.liu@mail.utoronto.ca
 */
public class JobObject {

	public Integer jobId;
	public Integer qValue;
	public String inputFile; 

	public final String DELIMITER = ":";
	public final String JOBPOOL_PATH = "/jobpool";

	// constuctors
	public JobObject(){};
    public JobObject(Integer jobId, Integer qValue) {
		this.jobId = jobId;
		this.qValue = qValue;
    };
    public JobObject(Integer jobId, Integer qValue, String inputFile) {
		this.jobId = jobId;
		this.qValue = qValue;
		this.inputFile = inputFile;
    };

	/**
	 * Parse znode data to JobObject
	 * @param jobString - znode data for a job
	 */
	public void parseJobString(String jobString) {
		String [] partial = jobString.split(DELIMITER);
		
		this.jobId = Integer.parseInt(partial[0]);
		this.qValue = Integer.parseInt(partial[1]);
		this.inputFile = partial[2];
	}

	/**
	 * Get the znode name of the JobObject.
	 */
	public String getJobNodeName() {
		return jobId.toString() + "-" + qValue.toString();
	}

	/**
	 * Get the zookeeper path to the parent of this job
	 */
	public String getJobpoolParentPath() {
		return JOBPOOL_PATH + "/" + jobId.toString();
	}

	/**
	 * Get the zookeeper path to this job
	 */
	public String getJobpoolPath() {
		return getJobpoolParentPath() + "/" + getJobNodeName();
	}

	/**
	 * Get the string representation of the job to store in znode.
	 */
	public String toJobDataString() {
		return jobId.toString() + DELIMITER + qValue.toString() + DELIMITER + inputFile;
	}
}
