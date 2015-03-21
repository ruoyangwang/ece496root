/*
 *WorkerThreadHandler is thread created from Worker.java
 *Its main responsibility is to execute NPAIRS with file input name and Q value
 *after it is done, it will return and create freeWorker and Result 
 *	@Author Ruoyang (Leo) Wang, ruoyang.wang@mail.utoronto.ca
*/
import java.net.*;
import java.io.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class WorkerThreadHandler implements Runnable {
		//the name of the dataset that we want to execute on
		String inputName;
		String currentJob;
		//Thread unique ID
		static private int threadNumber=0;
		int Qvalue;
		int jobID;

		@Override
		public void run() {
				long startTime = System.currentTimeMillis();
				int retcode = -1;					     			
				 try{ 
													
					System.out.println("executing jobs.....");

					String command = "sh ../execute/execute.sh "+this.inputName + " "+this.Qvalue;

					/* Script from run_NPAIRS.sh will detect if the execution of this job has an issue
					 * return code will be 1 or 2 if error occur
					 * Worker re-execute the same job until a success execution (return code is 0)
					 */	
					while(retcode!=0){
						Process p = Runtime.getRuntime().exec(command);
						retcode=p.waitFor();

					}	
						
				} catch (Exception e) {
						e.printStackTrace();
				}	
												
					//calculate this job's execution time		 	               	 	
					long endTime = System.currentTimeMillis();	                            		
					long executionTime = (endTime - startTime);
					Worker.Thread_complete(executionTime, retcode, this.currentJob, this.threadNumber, this.Qvalue, this.inputName, this.jobID);
		}
		
		public void setVal(String input, int val, String currentJob, int jobid){
			this.inputName = input;
			this.Qvalue = val;
			this.currentJob = currentJob;
			this.threadNumber+=1;
			this.jobID=jobid;
		
		}

		/*Not yet used */
		public int get_thread_number(){
			return this.threadNumber;
		}
		
		
}
