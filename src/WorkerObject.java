/* Each WorkerObject stores information of this computer node (Worker)'s information
 *
 *
 * Responsibility for WorkerObject:
 * 1.Reading the maximum job that this worker can execute in parallel from a pre-defined file
 * 2.setter/getter of this node's Host Name
 * 3.Executing NPAIRS initializing script (without Qvalue)
 * 4.Parse string information of this Node from Zookeeper
 * 5.Get the string representation of the data in the znode

 *	@Author Ruoyang (Leo) Wang, ruoyang.wang@mail.utoronto.ca
 */
import java.util.*;
import java.net.*;
import java.io.*;
public class WorkerObject {

	//whether to enable debug mode for I/O, change flag to false will disable all print statements
	private static boolean DEBUG =true;
	// name of the worker. ie: dir name for this worker
	private String workerName;
	public String freeWorkerNodeName;
	
	// performance metrics -- not yet used
	public long benchmarkTime;
	public long executionTime;
	public String cpucore;
	public String memFree;
	public final String DELIMITER = ":";
	
	//maximum Job that this worker can execute in parallel, represents this node's computation power
	public int maxJobNum;

	public String hostName;
	
	//constructor, mock a name as "temp" before getting real assigned name from Zookeeper
	public WorkerObject(){
		this.workerName = "temp";
	}

	public WorkerObject(String workerName) {
		// worker name including the id
		this.workerName = workerName;
	}

	// Get the name for the znode of this worker.
	public String getNodeName() {
		return workerName;
	}

	public void setNodeName(String wkname){
		this.workerName = wkname;


	}

	//return the maximum number of jobs this node can execute in parallel
	public int get_MaxJobNum(){

		return this.maxJobNum;


	}

	
	
	public void setHostName(String hostName) {
		this.hostName = hostName;	
	}

	public String getHostName() {
		return this.hostName;
	}

	
	public int Node_power(String filename){

		if(DEBUG)
			System.out.println("check NodePower, filename is:   ----   "+filename);

		try{
			File f = new File("../NPAIRS/init_NPAIRS.sh");
			if(!f.exists() || f.isDirectory()){
				Process p = Runtime.getRuntime().exec("cat /proc/meminfo |grep MemFree");
					p.waitFor();		//create shell object and retrieve cpucore number
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			
				while (br.readLine() != null){					//looking for FreeMem row 
					String[] tokens = br.readLine().split("\\s+");
					if(tokens[0].equals("MemFree:")){
						//getting the total freeMemory that can be used on this node
						this.memFree=tokens[1];
						break;
					}
				}
			
				BufferedReader fbr = new BufferedReader(new FileReader(new File("../system_config/memory_config.txt")));
				int minimum_require = Integer.parseInt(fbr.readLine());
				
				this.maxJobNum = Math.min(Integer.parseInt(this.memFree)/minimum_require,Integer.parseInt(cpucore));
				return Math.min(Integer.parseInt(this.memFree)/minimum_require,Integer.parseInt(cpucore));
			}
			else{
				if(DEBUG)
					System.out.println("start to execute init_NPAIRS script.......");
				//String command = "sh ../NPAIRS/init_NPAIRS.sh "+filename+" &> ~/init_npairs_runtime.log";	
				String command = "sh ../NPAIRS/init_NPAIRS.sh "+filename;	
				Process p = Runtime.getRuntime().exec(command);

				//getting runtime log
				StreamGobbler errLog = new StreamGobbler(p.getErrorStream() ,"ERROR");
				StreamGobbler outputLog = new StreamGobbler(p.getInputStream() ,"OUTPUT");

				errLog.start();
				outputLog.start();

				int retcode = p.waitFor();
				
				/*retrieving Node power from a local file pre-defined*/
				if(retcode == 0){
					if(DEBUG)
						System.out.println("start to run Benchmark...");
					
					f = new File("../NPAIRS/log/maxJobs.info");
					if(f.exists()){
						if(DEBUG)
							System.out.println("found the maxJobs file, read the number");
						BufferedReader fbr = new BufferedReader(new FileReader(f));
						this.maxJobNum= Integer.parseInt(fbr.readLine());
						return this.maxJobNum;
					}
				}
				
			}
			
		
		}catch (Exception e) {
            e.printStackTrace();
			return -1;
        }
		return -1;

	}
	
		
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

		
	// Get the string representation of the data in the znode.
	public String toNodeDataString() {
		String buffer = this.workerName+ this.DELIMITER+this.hostName+this.DELIMITER+ this.cpucore+ this.DELIMITER+this.executionTime;
		return buffer;
	}

}

