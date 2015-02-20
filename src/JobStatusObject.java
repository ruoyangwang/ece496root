public class JobStatusObject {
	public Integer totalJobs;
	public String status;
	public Long startTime; // System.currentTimeMillis()
	public Long endTime;
	public Long executionTimeM; // in min
	public Long executionTimeH; // in hours

	public final String COMPLETED = "COMPLETED";
	public final String KILLED = "KILLED";
	public final String ACTIVE = "ACTIVE";

	public final String DIVIDER = ":";

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


	public void killed() {
		this.status = KILLED;
	}

	public void completed() {
		this.status = COMPLETED;
		this.endTime = System.currentTimeMillis();
		this.getExecutionTime();
	}


	public Long getExecutionTime() {
		if (endTime != null && startTime != null) {
			this.executionTimeM = (endTime - startTime) / 60000L;
			this.executionTimeH = this.executionTimeM / 60L;
		}

		return executionTimeM;
	}

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
