public class JobObject {

	public Integer jobId;
	public Integer nValue;

	// location of the input file for the job.
	public String inputFile; 

	public Long startTime;

	// location of the output file for the job.
	public String outputFile; 
	// estimated time by the scheduler.
	public Integer estimatedTime;
	// actual completion time
	public Integer completionTime;
	// reqirement for this job
	public String requirement;
//*******************************************************************************

	public final String DELIMITER = ":";

	public final String JOBPOOL_PATH = "/jobpool";

	// constuctors
	public JobObject(){};

    public JobObject(Integer jobId, Integer nValue) {
		this.jobId = jobId;
		this.nValue = nValue;
    };

    public JobObject(Integer jobId, Integer nValue, String inputFile) {
		this.jobId = jobId;
		this.nValue = nValue;
		this.inputFile = inputFile;
    };

	// Parse job data string into object.
	public void parseJobString(String jobString) {
		if (jobString == null) {
			return;	
		}
		String [] partial = jobString.split(DELIMITER);
		try{
			this.jobId = Integer.parseInt(partial[0]);
			this.nValue = Integer.parseInt(partial[1]);
			this.inputFile = partial[2];
			this.startTime = Long.parseLong(partial[3]);
		} catch(Exception e) {
			;
		}
	}

	public void recordStartTime() {
		this.startTime = System.currentTimeMillis(); 
	}



	// Parse result data string into object.
	// TODO: this is not yet used
	public void parseResultString(String resultString) {
		String [] partial = resultString.split(DELIMITER);
		
		this.jobId = Integer.parseInt(partial[0]);
		this.nValue = Integer.parseInt(partial[1]);
		this.outputFile = partial[2];
		this.completionTime = Integer.parseInt(partial[3]);
	}

	public String getJobNodeName() {
		return jobId.toString() + "-" + nValue.toString();
	}

	public String getJobpoolParentPath() {
		return JOBPOOL_PATH + "/" + jobId.toString();
	}

	public String getJobpoolPath() {
		return getJobpoolParentPath() + "/" + getJobNodeName();
	}



	// Get the string representation of the data in the znode
	// when this job is in jobpool/worker
	public String toJobDataString() {
		// Formate: "jobId:nValue:inputFile"
		return jobId.toString() + DELIMITER + nValue.toString() + DELIMITER + inputFile + DELIMITER + startTime.toString();
	}
	

	// TODO: this is not yet used
	// Get the string representation of the data in the znode
	// when this job is in result
	public String toResultDataString() {
		// Formate: "jobId:nValue:outputFile:completionTime"
		return jobId.toString() + DELIMITER + nValue.toString() + DELIMITER + outputFile + DELIMITER + completionTime.toString();
	}

}
