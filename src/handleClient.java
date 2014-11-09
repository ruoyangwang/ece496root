import java.net.*;
import java.io.*;
import java.util.*;

public class handleClient extends Thread{

	private Socket socket = null;
	ObjectInputStream fromClient = null;
	private ObjectOutputStream toClient = null;
	
	
	// Constructor
	public handleClient(Socket socket) {
		super("handleClient");
		this.socket = socket;
		System.out.println("Created a new Thread to handle client");

	}
	



	private String fetchResult(String seqNum){

			// get result 

			String result = jobTracker.handleResult(seqNum);
			
			
	        String notF = "fail...";
			String inProg = "inprogress...";
			if(result == null){
				System.out.println("ERROR in fetchResult");
				result = "ERROR";
			}else if(result.equalsIgnoreCase(inProg)){

				result = "Still Processing Request: "+seqNum;

			}else if(result.equalsIgnoreCase(notF)){

				result = "The hash is not found";
			
			}else{
				result = "The result is: "+result;

			}
		
			return result;


	}


	private String newRequest(String request){

			// make a job id assign partition numbers
			Integer seqNum = jobTracker.getSequenceNum();
			

			// collect all workers



			Hashtable<String,String> newWork=new Hashtable();

			
			int i; 
			for (i=1;i<35;i++){

				newWork.put(seqNum.toString().trim()+"-"+i,request);

			}
			// put requests in zookeeper
			jobTracker.assignNewWork(newWork,seqNum.toString());

			return seqNum.toString();

	}



	public void run() {

		try {

			/* stream to read from client */
			
			String packetFromClient;
			/* stream to write back to client, but we don't use it here.
			 * We define it here so that during registration it can be saved 
			 * into the server Hashtable.
			 */

			try{	
			    System.out.println("Create a ObjectInputStream to handle Client");
			    fromClient = new ObjectInputStream(socket.getInputStream());
			}
			catch(Exception e){
			    System.err.println("Error: Unable to create inputStream at ClientHandler Constructor");
			}
	System.out.println("Wait for request");
			packetFromClient = (String) fromClient.readObject();
	System.out.println("Got a request");
			String packetToClient;
			String[] temp = packetFromClient.split(":");
			if(temp[0].equalsIgnoreCase(new String("req"))){
 				System.out.println("A New Request");
 				packetToClient="Tracking ID: "+newRequest(temp[1]);


			}else if(temp[0].equalsIgnoreCase(new String("seq"))){

				System.out.println("A Fetch Request");
				packetToClient=fetchResult(temp[1]);

			}else{
				System.out.println("Unknown Request");

				packetToClient= "Unknown";
			}


			toClient = new ObjectOutputStream(socket.getOutputStream());	
						
			toClient.writeObject(packetToClient);
			
			toClient.close();

			fromClient.close();
			socket.close();
			System.out.println("Quitting - result sent to client");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
