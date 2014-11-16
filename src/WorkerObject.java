public class WorkerObject {

	// name of the worker. ie: dir name for this worker
	public String workerName;

	// performance metrics -- not yet used
	public Integer benchmarkTime;

	public final String DELIMITER = ":";

	public WorkerObject(String workerName) {
		// worker name including the id
		this.workerName = workerName;
	}

	// Get the name for the znode of this worker.
	public String getNodeName() {
		return workerName;
	}

	// TODO: this is not yet used		
	// Parse the data string to get object representation
	public void parseNodeString(String nodeDataString) {
		benchmarkTime = Integer.parseInt(nodeDataString);
	} 

	// TODO: this is not yet used		
	// Get the string representation of the data in the znode.
	public String toNodeDataString() {
		return benchmarkTime.toString();
	}

}
