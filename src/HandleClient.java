import java.net.*;
import java.io.*;
import java.util.*;

/**
 * Responsibilities of HandleClient:
 * 1. Handle new job submission from client
 * 2. Handle status look up from client
 * 3. Handle job killing requests from client
 * 4. Handle adding workers request from client
 *
 * NOTE: We only allow one job to be executed at a time. Because
 *		 workers are initialized for the particular job submitted,
 *		 we can only accept one job at a time until there is a
 *		 way to ensure workers would not get assign jobs they
 *		 are not initialized for.
 *
 * @author Jie (Jacky) Liu, jjiiee.liu@mail.utoronto.ca
 */
public class HandleClient extends Thread {

	private Socket socket = null;
	ObjectInputStream fromClient = null;
	private ObjectOutputStream toClient = null;

	final String RUN = "run";
	final String STATUS = "status";
	final String KILL = "kill";
	final String ADD = "add";

	static boolean DEBUG;

	/** 
	 * Constructor for handle client.
	 * @param socket - the socket client is listening on
	 */
	public HandleClient(Socket socket) {
		super("HandleClient");
		this.socket = socket;
		if (DEBUG)
			System.out.println("Created a new Thread to handle client");
	}

	/**
	 * Convert a comma separated string to a list.
	 * @param str - comma separated string
	 * @return - a list of the items
	 */
	private List<String> stringToList (String str) {
		List<String> list = new ArrayList<String>();
		for(String item: str.split(",")) {
			list.add(item);
		}
		return list;
	}

	/**
	 * Convert a list of items to comma separated string.
	 * @param list - a list of item to be converted.
	 * @param - comma separated items from the list
	 */
	private String listToString(List<String> list) {
		StringBuilder sb = new StringBuilder();
		for (String l : list) {
			if (sb.length() == 0) {
				sb.append(l);
			} else {
				sb.append(", ");
				sb.append(l);
			}
		}
		return sb.toString();
	}


	/**
	 * Start workers on remote hosts.
	 * @param host - a list of remote hosts to start workers on
	 * @param inputFile - input file of the current execution
	 * @param jobId - job id of the current execution
	 */	
	private void startWorkers(List<String> hosts, String inputFile, String jobId) {
		String zkHost = JobTracker.getZookeeperHost();
		for(String host: hosts) {
			String command = "sh ../execute/startWorkers.sh " + host + " " + zkHost + " " + inputFile + " " + jobId;
			if (DEBUG)		
				System.out.println("Starting worker on " + host + " with command: " + command);

			try {
				Process p = Runtime.getRuntime().exec(command);
			} catch(Exception e) {
				System.out.println("Exception at startWorkers with command:" + command);
			}
		}		
	}
	
	/**
	 * Submit a new job to zookeeper. Create the /jobpool/{jobId} znode. 
	 * Determines the Q value range from user input and create znodes under
	 * /jobpool/{jobId}
	 *
	 * @param inputFileName - input file name to execute.
	 * @param qValues - the unparsed Q values string entered by client. 
	 * @return - job id of the created job.
	 */
	private String newJob(String inputFileName, String qValues){

		// get a job id
		Integer jobID = JobTracker.getSequenceNum();
		
		// parse qValues
		ArrayList<Integer> qValueList = new ArrayList<Integer>();
		
		for(String qPartial: qValues.split(",")) {
			List<String> range = Arrays.asList(qPartial.split("-"));
			if (range.size() == 1) {
				// A single value, not a range
				qValueList.add(new Integer(range.get(0)));
			} else {
				// A range
				int from = Integer.parseInt(range.get(0));
				int to = Integer.parseInt(range.get(1));
				while (from <= to) {
					qValueList.add(new Integer(from));
					from ++;
				}
			} 
		}
		
		// Create job id znode under jobpool
		JobTracker.addJobIdToPool(jobID.toString(), qValueList.size());

		// Create individual jobs in /jobpool/{jobId}
		for (Integer q: qValueList) {
			JobObject j = new JobObject(jobID, q, inputFileName);
			JobTracker.addToJobPool(j);
		}

		return jobID.toString();
	}


	/**
	 * Handle client requests.
	 */
	public void run() {
		// Set debug flag the same as JobTracker
		DEBUG = JobTracker.DEBUG;

		try {
			boolean waitForWorkerCreation = false;
			String packetFromClient;
			try{	
				if (DEBUG)
				    System.out.println("Create a ObjectInputStream to handle Client");
			    fromClient = new ObjectInputStream(socket.getInputStream());
			}
			catch(Exception e){
			    System.err.println("Error: Unable to create inputStream at ClientHandler Constructor");
			}

			if (DEBUG)
				System.out.println("Wait for request");
			
			// read client input
			packetFromClient = (String) fromClient.readObject();
	
			if (DEBUG)
				System.out.println("Got a request");

			String packetToClient;
			// split the request.
			String[] partialRequest = packetFromClient.split(":");

			// New job submission
			if(partialRequest[0].equalsIgnoreCase(RUN)){
				if (DEBUG)
	 				System.out.println("A New Request: " + packetFromClient);
				
				String inputFileName = partialRequest[1];
				String qValues = partialRequest[2];
				String workers = partialRequest[3];
				String runningJob = JobTracker.getRunningJob();

				// Only allow one job to be executed at a time. Because
				// workers are initialized for the particular job submitted,
				// we can only accept one job at a time until there is a
				// way to ensure workers would not get assign jobs they
				// are not initialized for.
				if (runningJob != null) {
					packetToClient="Job " + runningJob + " is not completed.";
				} else {

					String jobId = newJob(inputFileName, qValues);

					// set current job in job tracker
					JobTracker.setCurrentJob(inputFileName, jobId);

					List<String> allWorkers = stringToList(workers);
					List<String> startedWorkers = JobTracker.workersNotStarted(allWorkers);
					startWorkers(allWorkers, inputFileName, jobId);

					waitForWorkerCreation = true;
	 				packetToClient="Tracking ID: " + jobId;
				}
			// Requesting status of a job
			} else if(partialRequest[0].equalsIgnoreCase(STATUS)) {
				if (DEBUG)
					System.out.println("A New Request: " + packetFromClient);
				
				String jobId = partialRequest[1];
				int result = JobTracker.checkStatus(jobId);
				packetToClient =  "Job ID - " + jobId + ":";

				if (result == 1) {
					JobStatusObject jso = JobTracker.getCompletedJobStatus(jobId);
					String t = null;
					if (jso == null) {
						t = new String("Job "+jobId+" is either killed or not completed");
					} else {
						long min = jso.executionTimeM;
						long hr = jso.executionTimeH;
						t = new String("Job "+jobId+" took "+hr+" hours ("+min+" min)");	
					}
					packetToClient = t;
				} else if (result == 2) {
					packetToClient =  packetToClient + " Was killed.";
				} else if (result == 0) {
					packetToClient =  packetToClient + " Not Finished.";
				} else {
					packetToClient =  packetToClient + " Error occured. Please see log";
				}

			// Killing a job
			} else if(partialRequest[0].equalsIgnoreCase(KILL)) {
				if (DEBUG)
					System.out.println("A New Request: " + packetFromClient);
				
				String jobId = partialRequest[1];
				JobTracker.killJob(jobId);
				packetToClient =  "Job - " + jobId + " killed";
				
			// Adding a worker to current job
			} else if (partialRequest[0].equalsIgnoreCase(ADD)) {
				
				String workers = partialRequest[1];
				String inputFileName = JobTracker.CurrentJobFile;
				String jobId = JobTracker.CurrentJobId;
				List<String> allWorkers = stringToList(workers);
				// Need to make sure the worker is not already started.
				List<String> startedWorkers = JobTracker.workersNotStarted(allWorkers);

				startWorkers(allWorkers, inputFileName, jobId);
				waitForWorkerCreation = true;
				packetToClient="Hosts " + listToString(allWorkers) + " added to computation cluster.\n";
				
				if (startedWorkers.size() > 0) {
					packetToClient = packetToClient + listToString(startedWorkers) + " were already started.";
				}

			} else {
				System.out.println("Unknown Request: " + packetFromClient);
				packetToClient= "Unknown request";
			}

			// Write to client and close sockets.
			toClient = new ObjectOutputStream(socket.getOutputStream());	
			toClient.writeObject(packetToClient);
			toClient.close();
			fromClient.close();
			socket.close();

			if (DEBUG)
				System.out.println("Quitting - result sent to client");

			// Sleep for the creation of workers.
			if (waitForWorkerCreation) {
				try {
					sleep(5000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
