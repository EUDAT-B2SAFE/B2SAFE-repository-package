package fr.cines.eudat.repopack.b2safe_rp_pilot;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

import fr.cines.eudat.log.Log;
import fr.cines.eudat.repopack.b2safe_rp_core.*;

public class B2safeRpPilot {
	// constants
	public static final String versionInfo = "v1.1.0 - Work in progress   ";
	
	public static final boolean LOG_TRACE = false;
    protected static Logger log=null;

    public static Properties prop=null;
    private static InputStream input =null;
    private static Scanner scanner = new Scanner (System.in);

    private static FileBasedInterface fileBasedInterface = new FileBasedInterface();
	private static DataSet dataSet = null;

	public static void main(String[] args) {
		try {
			new Log();
			log= Log.getLogger(DataSet.class.getName());

			init();
			dataSet = new DataSet(prop);
			if (prop.getProperty("PILOT_EXEC_MODE").trim().equals("console")) {
				// Launch menu
				log.info("Launching console mode with version : "+ versionInfo);
				while (interactiveMenu());		
				log.info("Leaving console mode with version : "+ versionInfo);
			}
			else {
				if (prop.getProperty("PILOT_EXEC_MODE").trim().equals("batch")) {
					log.info("Launching batch mode with version : "+ versionInfo);
					batchExecution();
					log.info("Ending batch mode");				
				}
			}
			if (dataSet != null) dataSet.closeConnection();
		} catch (FileNotFoundException ex) {
			log.error(ex.getMessage());
		} catch (IOException ex) {
			log.error(ex.getMessage());
		}		
	}

	private static void init() {
        
		// Load the properties from file
        prop= new Properties();		
		try {
			input = new FileInputStream("config.properties");
			if(input==null) 
			{
				log.error("Unable to find config.properties");
			}
			else
			{
				prop.load(input);
				log.debug("init OK");
			}

		} catch (FileNotFoundException ex) {
			log.error(ex.getMessage());
		} catch (IOException ex) {
			log.error(ex.getMessage());
		}

	}
	
	private static boolean interactiveMenu() throws IOException {
	    int swValue;
	    String dataEntry;

	    // Display menu graphics
	    System.out.println("=========================================================");
	    System.out.println("|   EUDAT repository packages                           |");
	    System.out.println("|       Console based application                       |");
	    System.out.println("|       "+ versionInfo +"                         |");
	    System.out.println("=========================================================");
	    System.out.println("| Options:                                              |");
	    System.out.println("|        1. Test Connection to B2SAFE                   |");
	    System.out.println("|        2. Replicate based on ToReplicate file         |");
	    System.out.println("|        3. Delete based on ToDelete file (TEST ONLY)   |");
	    System.out.println("|        4. Retrieve files based on ToRetrieve file     |");
	    System.out.println("|        5. List files contained in a directory         |");
	    System.out.println("|        6. List B2SAFE metadata for one file           |");
	    System.out.println("|        9. Test handling of DO file                    |");
	    System.out.println("|        0. Exit                                        |");
	    System.out.println("=========================================================");
	    System.out.println("Enter your choice : ");
	    swValue = scanner.nextInt();

	    // Switch construct
	    switch (swValue) {
	    case 1:
	    	System.out.println(testConnection()==true ? "Connected" : "Not connected");
	    	if (dataSet.isInitialized()) System.out.println(dataSet.getServerInformationToString());
	    	break;
	    case 2:
	    	batchExecution();
	    	break;
	    case 3:
	    	if (testConnection()) 
	    		fileBasedInterface.writeOperationResultToFile(dataSet.deleteAllRequestedDO(fileBasedInterface.initToDeleteDOList()));
	    	break;
	    case 4:
	    	if (dataSet.initB2safeConnection()) 
	    		fileBasedInterface.writeOperationResultToFile(dataSet.retrieveListOfDOByPath(fileBasedInterface.initToRetrieveDOList()));
	    	break;
	    case 5:
		    System.out.println("Enter collection path : ");
		    dataEntry = scanner.next();
	    	if (dataSet.initB2safeConnection()) {
				for (DataObject dataObject : dataSet.listDOFromDirectory(dataEntry, false)) {
					System.out.println(dataObject.toString());
				}
			}
	    	break;
	    case 6:
		    System.out.println("Enter file path (relative to base directory) : ");
		    dataEntry = scanner.next();
		    DataObject tmpDO = new DataObject();
		    tmpDO.setFileName(dataEntry);
		    tmpDO.setRemoteDirPath("");
		    
	    	if (testConnection()) {
				System.out.println(dataSet.getMetadataFromOneDOByPath(tmpDO).toString());
			}
	    	break;
	    case 9:
			for (DataObject dataObject : fileBasedInterface.initToReplicateDOList()) {
				System.out.println(dataObject.toString());
			}
			// fileBasedInterface.writeReplicaResultToFile(fileBasedInterface.initToReplicateDOList());
	    	break;
	    case 0:
	    	System.out.println("Exit selected");
	    	return false;
	    default:
	    	System.out.println("Invalid selection");
	    	return false;
	    }
	    return true;
	}

	private static boolean batchExecution() {
		// The batch mode launches the Replication based on the available file
		// The replication is launched file by file
		ArrayList<DataObject> ongoingDOList = new ArrayList<DataObject>();

		// Get the list from file
		ArrayList<DataObject> toReplicateDOList = fileBasedInterface.initToReplicateDOList();
		// Loop on the list to launch file by file, and write result after each execution
		for (DataObject dataObject : toReplicateDOList) {
			ongoingDOList.add(dataObject);
			fileBasedInterface.writeOperationResultToFile(dataSet.replicateAllRequestedDO(ongoingDOList));
			ongoingDOList.clear();
		}
		return true;
	}
	
    /*
     * 
     */
    public static boolean testConnection() {
    	if (dataSet.isInitialized()) {
    		log.debug("Already connected to B2SAFE ");
    		return true;
    	}
    	else {
    		if (dataSet.initB2safeConnection()) {
    			log.debug("Successful connection to B2SAFE ");
    			return true;
    		}
    		else {
    			log.debug("Error connecting to B2SAFE ");
    			return false;
    		}
    	}
    }
}
