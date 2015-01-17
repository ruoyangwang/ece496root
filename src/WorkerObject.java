import java.util.*;
import java.net.*;
import java.io.*;
public class WorkerObject {

	// name of the worker. ie: dir name for this worker
	private String workerName;

	// performance metrics -- not yet used
	public long benchmarkTime;
	public long executionTime;
	public String cpucore;
	public String memFree;
	public final String DELIMITER = ":";

	public WorkerObject(String workerName) {
		// worker name including the id
		this.workerName = workerName;
	}

	// Get the name for the znode of this worker.
	public String getNodeName() {
		return workerName;
	}

	public int Node_power(){
		try{
			Process p = Runtime.getRuntime().exec("cat /proc/meminfo |grep MemFree");
    			p.waitFor();		//create shell object and retrieve cpucore number
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			
			while (br.readLine() != null){					//looking for FreeMem row 
				String[] tokens = br.readLine().split("\\s+");
				System.out.println(tokens[0]);
				if(tokens[0].equals("MemFree:")){
					this.memFree=tokens[1];
					break;
				}
			}
			
			BufferedReader fbr = new BufferedReader(new FileReader(new File("../system_config/memory_config.txt")));
			int minimum_require = Integer.parseInt(fbr.readLine());
			return Integer.parseInt(this.memFree)/minimum_require;
		
		}catch (Exception e) {
            e.printStackTrace();
			return -1;
        }

	}
	/*public void setHardwareInfo(String[] temp){
		hardwareInfo = Arrays.copyOf( temp, temp.length );
	}*/
	// TODO: this is not yet used		
	// Parse the data string to get object representation
	public void parseNodeString(String nodeDataString) {
		if (nodeDataString == null){
			return;
		}
		String[] tokens = nodeDataString.split(DELIMITER);
		this.workerName= tokens[0];
		this.cpucore=tokens[1];
		this.executionTime=Long.valueOf(tokens[2]).longValue();
	} 

	// TODO: this is not yet used		
	// Get the string representation of the data in the znode.
	public String toNodeDataString() {
		String buffer = this.workerName+ this.DELIMITER+ this.cpucore+ this.DELIMITER+this.executionTime;
		return buffer;
	}

}
