import java.net.*;
import java.io.*;
import java.util.*;

public class Listener extends Thread{

		static ServerSocket socket;

		public Listener (String port){
			super("Listener");
			try{
		    	socket = new ServerSocket(Integer.parseInt(port));
			}catch (Exception e){
				System.out.println("Failed to create server socker");

			}	
		System.out.println("Created a new Listener");
		
		}

		public void run(){





			/*System.out.println("Sleeping...");
			try{Thread.sleep(10000);
        		
			}catch (Exception e){
				;
			}		   	

System.out.println("Creating worker for test...");
jobTracker.test();*/
			while(true){
				try{
					System.out.println("Listening...");
					new handleClient(socket.accept()).start();   
				}catch (Exception e){}
			}
		}


	}
