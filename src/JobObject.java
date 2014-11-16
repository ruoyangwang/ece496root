public class JobObject {

	public Integer jobId;
	public Integer nValue;

	// location of the input file for the job.
	public String inputFile; 
	// location of the output file for the job.
	public String outputFile; 
	// estimated time by the scheduler.
	public Integer estimatedTime;
	// actual completion time
	public Integer completionTime;

	public final String DELIMITER = ":";

	// constuctor
    public JobObject(Integer nValue, String inputFile) {
		this.nValue = nValue;
		this.inputFile = inputFile;
    };

    public JobObject(int jobId, Integer nValue, String inputFile) {
		this.jobId = jobId;
		this.nValue = nValue;
		this.inputFile = inputFile;
    };

	public void parseJobString(String jobString) {
		String [] partial = jobString.split(DELIMITER);
		
		this.nValue = Integer.parseInt(partial[0]);
		this.inputFile = partial[1];
	}

	public void parseResultString(String resultString) {
		String [] partial = resultString.split(DELIMITER);
		
		this.nValue = Integer.parseInt(partial[0]);
		this.outputFile = partial[1];
		this.completionTime = Integer.parseInt(partial[2]);
	}

	public String toJobString() {
		// Formate: "nValue:inputFile"
		return nValue.toString() + DELIMITER + inputFile;
	}
	
	public String toResultString() {
		// Formate: "nValue:outputFile:completionTime"
		return nValue.toString() + DELIMITER + outputFile + DELIMITER + completionTime.toString();
	}

}
