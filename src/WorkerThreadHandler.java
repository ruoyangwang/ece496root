import java.net.*;
import java.io.*;

public class WorkerThreadHandler extends Thread {
		String inputLocation;
		String currentJob;
		int Qvalue;

		public void start() {
				long startTime = System.nanoTime();
				int retcode = -1;					     			
				 try{ 
													//mock of execution, depends on where we put zookeeper and NPAIRS executables we can change shell command 
					System.out.println("executing jobs.....");
					String command = "sh ../execute/execute.sh " + this.inputLocation+" "+ this.Qvalue;				
					Process p = Runtime.getRuntime().exec(command);
					retcode=p.waitFor();
									
					} catch (Exception e) {
						e.printStackTrace();
					}	//TODO:assume this is running the child node for now
												
							                	 	
					long endTime = System.nanoTime();	                            		
					long executionTime = (endTime - startTime);
					Worker.Thread_complete(executionTime, retcode, this.currentJob);
		}
		
		public void setVal(String input, int val, String currentJob){
			this.inputLocation = input;
			this.Qvalue = val;
			this.currentJob = currentJob;
		
		}
		
		
}
