import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class test{	
	String temp;
	long executionTime;
	
	public static void main(String[] args){
		test wk = new test();
		wk.temp = wk.addToFreeWorker();
		System.out.println(wk.temp);


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
}
