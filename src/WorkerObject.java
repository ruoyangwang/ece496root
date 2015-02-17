
import java.util.*;
import java.net.*;
import java.io.*;
public class WorkerObject {

	// name of the worker. ie: dir name for this worker
	private String workerName;
	public String freeWorkerNodeName;

	// performance metrics -- not yet used
	public long benchmarkTime;
	public long executionTime;
	public String cpucore;
	public String memFree;
	public final String DELIMITER = ":";
	public int maxJobNum;

	public String hostName;
	
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
						this.memFree=tokens[1];
						break;
					}
				}
			
				BufferedReader fbr = new BufferedReader(new FileReader(new File("../system_config/memory_config.txt")));
				int minimum_require = Integer.parseInt(fbr.readLine());
				//System.out.println("test cpucore number...........");
				//System.out.println(cpucore);
				//Integer.parseInt(this.memFree)/minimum_require;
				this.maxJobNum = Math.min(Integer.parseInt(this.memFree)/minimum_require,Integer.parseInt(cpucore));
				return Math.min(Integer.parseInt(this.memFree)/minimum_require,Integer.parseInt(cpucore));
			}
			else{
				System.out.println("start to execute init_NPAIRS script.......");
				String command = "sh ../NPAIRS/init_NPAIRS.sh "+filename;	
				/*initialized script*/			
				Process p = Runtime.getRuntime().exec(command);
				int retcode = p.waitFor();

				/*now run benchmark*/
				if(retcode == 0){
					System.out.println("start to run Benchmark...");
					/*String command2 = "sh ../NPAIRS/bin/npairs_multiProc_benchmark.sh";
					p = Runtime.getRuntime().exec(command2);
					p.waitFor();*/
					f = new File("../NPAIRS/log/maxJobs.info");
					if(f.exists()){
						System.out.println("found the maxJobs file, read the number");
						BufferedReader fbr = new BufferedReader(new FileReader(f));
						this.maxJobNum= Integer.parseInt(fbr.readLine());
						return this.maxJobNum;
					}
				}
			}
			System.out.println("error occurs return 1 freeWorker");
			return 1;
		
		}catch (Exception e) {
            e.printStackTrace();
			return -1;
        }

	}
	
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
		String buffer = this.workerName+ this.DELIMITER+this.hostName+this.DELIMITER+ this.cpucore+ this.DELIMITER+this.executionTime;
		return buffer;
	}

}

