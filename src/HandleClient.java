import java.net.*;
import java.io.*;
import java.util.*;

public class HandleClient extends Thread{

	private Socket socket = null;
	ObjectInputStream fromClient = null;
	private ObjectOutputStream toClient = null;
	final static String SSH_SCRIPT = "ssh";
	final static String WORKER_START_SCRIPT = "nohup ./worker.sh";	

	List<Process> processList = new ArrayList<Process>();
	// Constructor
	public HandleClient(Socket socket) {
		super("HandleClient");
		this.socket = socket;
		System.out.println("Created a new Thread to handle client");

	}

	private List<String> hostStringToList (String hosts) {
		List<String> hostList = new ArrayList<String>();
		for(String host: hosts.split(",")) {
			hostList.put(host);
		}
		return hostList;
	}

	private void startWorker(String host, String inputFile) {
		String workerScript = WORKER_START_SCRIPT + " " + JobTracker.ZookeeperLocation + " " + inputFile + "&> worker.log &";
		String command = "";
		if (host.equalsIgnoreCase(new String("localhost"))) {
			command = workerScript;

		} else {
			String sshScript = SSH_SCRIPT + " " + host;
			command = sshScript + " \"" + workerScript + "\"";
		}

		
		Process p = Runtime.getRuntime().exec(command);
		processList.add(p);
	}


	private void startWorkers(List<String> hosts, String inputFile) {
		for(String host: hosts) {
			startWorker(host, inputFile);
		}
		
		try {
			sleep(5000);
		} catch (Exception e) {;}
		
		for (Process p: processList) {
			p.destroy();
		}
		
	}

	
	private String newRequest(String inputFileName, String nValues){

			// get a job id
			Integer jobID = JobTracker.getSequenceNum();
			
			// parse nValues
			ArrayList<Integer> nValueList = new ArrayList<Integer>();
			
			for(String nPartial: nValues.split(",")) {
				List<String> range = Arrays.asList(nPartial.split("-"));
				if (range.size() == 1) {
					// is a single value, not a range
					nValueList.add(new Integer(range.get(0)));
				} else {
					// is a range
					int from = Integer.parseInt(range.get(0));
					int to = Integer.parseInt(range.get(1));
					while (from <= to) {
						nValueList.add(new Integer(from));
						from ++;
					}
				} 
			}
			
			// create dir under jobpool
			JobTracker.addJobIdToPool(jobID.toString(), nValueList.size());

			for (Integer q: nValueList) {
				
				JobObject j = new JobObject(jobID, q, inputFileName);
				JobTracker.addToJobPool(j);
			}

			return jobID.toString();
	}

	private int checkResult(String jobId) {
		return JobTracker.checkResult(jobId);
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


			// run:inputfile:N
			if(temp[0].equalsIgnoreCase(new String("run"))){
 				System.out.println("A New Request: " + packetFromClient);
				
				String inputFileName = temp[1];
				String nValues = temp[2];
				String workers = temp[3];

				String jobId = newRequest(inputFileName, nValues);

				startWorkers(workersNotStarted(hostStringToList(workers)), inputFileName);

				JobTracker.CurrentJobFile = inputFileName;

// TODO: check status............ and reset???



 				packetToClient="Tracking ID: " + jobId;

			} else if(temp[0].equalsIgnoreCase(new String("status"))) {
				System.out.println("A New Request: " + packetFromClient);
				
				String jobId = temp[1];
				int result = checkResult(jobId);
				packetToClient =  "Job ID - " + jobId + ":";
				if (result == 1) {
					packetToClient =  packetToClient + " Finished.";
				} else if (result == 0) {
					packetToClient =  packetToClient + " Not Finished.";
				} else {
					packetToClient =  packetToClient + " Error occured. Please see log";
				}
				
			} else if (temp[0].equalsIgnoreCase(new String("add"))) {
				
				String workers = temp[1];
				String inputFileName = JobTracker.CurrentJobFile;

				startWorkers(workersNotStarted(hostStringToList(workers)), inputFileName);
				packetToClient="Hosts " + workers + " added to computation cluster.";
			} else {
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
