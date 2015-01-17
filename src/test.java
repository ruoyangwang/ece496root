import java.util.List;
import java.net.*;
import java.io.*;

public class test{	
	String temp;
	private String workerName;
	long executionTime;
	String cpucore;
	public String memFree;
	String DELIMITER=":";

	public static void main(String[] args ){
		test wk = new test();
		/*wk.cpucore = wk.addToFreeWorker();
		System.out.println(wk.cpucore);
		System.out.println(wk.nodeToString());
		String holder=wk.nodeToString();
		String[] tokens = holder.split(wk.DELIMITER);
		wk.workerName= tokens[0];
		wk.cpucore=tokens[1];
		wk.executionTime=Long.valueOf(tokens[2]).longValue() ;
		System.out.println(wk.workerName);
		System.out.println(wk.executionTime);*/
		System.out.println(wk.Node_power());
	}
	

	public test(){
		this.workerName= "test 1";
		this.cpucore=null;
	}

	public String addToFreeWorker(){
		
		try{
			long startTime = System.nanoTime();
			Process p = Runtime.getRuntime().exec("getconf _NPROCESSORS_ONLN");
    			p.waitFor();
			BufferedReader br = 
                            new BufferedReader(new InputStreamReader(p.getInputStream()));
			long endTime = System.nanoTime();	                            		
			System.out.println(this.executionTime = (endTime - startTime));
			return br.readLine();
			
		}catch (Exception e) {
            e.printStackTrace();
            return null;
        }

	}

	public String nodeToString(){
		return this.workerName+ this.DELIMITER+ this.cpucore+ this.DELIMITER+this.executionTime;

	}
	
	public int Node_power(){
		try{
			Process p = Runtime.getRuntime().exec("cat /proc/meminfo |grep MemFree");
    			p.waitFor();		//create shell object and retrieve cpucore number
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while (br.readLine() != null){
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
}
