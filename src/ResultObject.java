/* Each ResultObject stores information of one execution of NPAIRS 
 * Important infomration include:
 *	-hostname of the node that executed this job
 *	-total execution Time of this job
 *	-result Location 
 *	-unqie JobID of this job
 *	-Q value used to execute this job

 *	@Author Ruoyang (Leo) Wang, ruoyang.wang@mail.utoronto.ca
 */



import java.util.*;
import java.net.*;
import java.io.*;
public class ResultObject {
	private String hostname;
	public String resultLocation;
	long executionTime;
	public int JobID;	
	int Q;
	static final String DELIMITER=":";
	
	public ResultObject(String hostname, String location, long ET, int JobID, int Q){
		this.hostname = hostname;
		this.resultLocation= location;
		this.executionTime=ET;
		this.JobID=JobID;
		this.Q=Q;	

	}
	
	//parser function that parse node serialized string 
	public void parseNodeString(String nodeDataString) {
		if (nodeDataString == null){
			return;
		}
		String[] tokens = nodeDataString.split(DELIMITER);
		this.hostname= tokens[0];
		this.resultLocation=tokens[1];
		this.executionTime=Long.valueOf(tokens[2]).longValue();
		this.JobID = Integer.parseInt(tokens[3]);
		this.Q = Integer.parseInt(tokens[4]);
		
	} 
		
	// Get the string representation of the data in the znode.
	public String toNodeDataString() {
		String buffer = this.hostname+ this.DELIMITER+ this.resultLocation+ this.DELIMITER+this.executionTime+ this.DELIMITER+this.JobID+ this.DELIMITER+this.Q;
		return buffer;
	}

	public String get_Result_Node_Name(){
		return JobID+"-"+Q;

	}
}
	
