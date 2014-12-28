import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class test{	
	String temp;
	private String workerName;
	long executionTime;
	String cpucore;
	String DELIMITER=":";

	public static void main(String[] args){
		test wk = new test();
		wk.cpucore = wk.addToFreeWorker();
		System.out.println(wk.cpucore);
		System.out.println(wk.nodeToString());
		String holder=wk.nodeToString();
		String[] tokens = holder.split(wk.DELIMITER);
		wk.workerName= tokens[0];
		wk.cpucore=tokens[1];
		wk.executionTime=Long.valueOf(tokens[2]).longValue() ;
		System.out.println(wk.workerName);
		System.out.println(wk.executionTime);
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
}
