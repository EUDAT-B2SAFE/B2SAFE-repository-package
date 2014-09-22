package fr.cines.eudat.repopack.b2safe_rp_pilot;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

import fr.cines.eudat.log.Log;
import fr.cines.eudat.repopack.b2safe_rp_core.*;

public class B2safeRpPilot {
	// constants
	public static final String versionInfo = "v1.0.0 - 17/9/14";
	
	public static final boolean LOG_TRACE = false;
    protected static Logger log=null;

    public static Properties prop=null;
    private static InputStream input =null;
    private static Scanner scanner = new Scanner (System.in);

    private static FileBasedInterface fileBasedInterface = new FileBasedInterface();
	private static DataSet dataSet = new DataSet();

	public static void main(String[] args) throws IOException {
		new Log();
        log= Log.getLogger(DataSet.class.getName());
		
		init();
		if (prop.getProperty("PILOT_EXEC_MODE").equals("console")) {
			// Launch menu
			while (interactiveMenu());		
		}
		else {
			if (prop.getProperty("PILOT_EXEC_MODE").equals("batch")) {
				log.info("Launching batch mode with version : "+ versionInfo);
				// The batch mode launches the Replication based on the available file
		    	fileBasedInterface.initToReplicateDOList();
		    	if (dataSet.initB2safeConnection()) 
		    		fileBasedInterface.writeOperationResultToFile(dataSet.replicateAllRequestedDO(fileBasedInterface.initToReplicateDOList()));
				log.info("Ending batch mode");				
			}
		}
	}
	
	private static void init() {
        
		// Load the properties from file
        prop= new Properties();		
		try {
			input = new FileInputStream("config.properties");
			if(input==null) 
			{
				log.debug("Unable to find config.properties");
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
	    System.out.println("|        9. Test handling of DO file                    |");
	    System.out.println("|        0. Exit                                        |");
	    System.out.println("=========================================================");
	    System.out.println("Enter your choice : ");
	    swValue = scanner.nextInt();

	    // Switch construct
	    switch (swValue) {
	    case 1:
	    	System.out.println(dataSet.testConnection());
	    	break;
	    case 2:
	    	fileBasedInterface.initToReplicateDOList();
	    	if (dataSet.initB2safeConnection()) 
	    		fileBasedInterface.writeOperationResultToFile(dataSet.replicateAllRequestedDO(fileBasedInterface.initToReplicateDOList()));
	    	break;
	    case 3:
	    	if (dataSet.initB2safeConnection()) 
	    		fileBasedInterface.writeOperationResultToFile(dataSet.deleteAllRequestedDO(fileBasedInterface.initToDeleteDOList()));
	    	break;
	    case 4:
	    	if (dataSet.initB2safeConnection()) 
	    		fileBasedInterface.writeOperationResultToFile(dataSet.retrieveListOfDOByPath(fileBasedInterface.initToRetrieveDOList()));
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

	
}
