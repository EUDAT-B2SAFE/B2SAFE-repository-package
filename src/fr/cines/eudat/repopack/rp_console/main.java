package fr.cines.eudat.repopack.rp_console;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

import fr.cines.eudat.log.Log;
import fr.cines.eudat.repopack.rp_core.DataObject;
import fr.cines.eudat.repopack.rp_core.DataSet;

public class main {
	public static final boolean LOG_TRACE = false;
	private static FileBasedInterface fileBasedInterface = new FileBasedInterface();
	private static DataSet dataSet = new DataSet();
    public static Properties prop=null;
    private static InputStream input =null;
    protected static Logger log=null;



	public static void main(String[] args) throws IOException {
		
		new Log();
        log= Log.getLogger(DataSet.class.getName());
        
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

				
		// Launch menu
		while (interactiveMenu());
	}
	
	private static boolean interactiveMenu() throws IOException {
	    int swValue;
	    Scanner scanner = new Scanner (System.in);

	    // Display menu graphics
	    System.out.println("=========================================================");
	    System.out.println("|   MENU SELECTION DEMO                                 |");
	    System.out.println("=========================================================");
	    System.out.println("| Options:                                              |");
	    System.out.println("|        1. Init Connection to B2SAFE                   |");
	    System.out.println("|        2. Replicate based on ToReplicate file         |");
	    System.out.println("|        3. Delete based on ToReplicate file (TEST)     |");
	    System.out.println("|        4. Test DO to string                           |");
	    System.out.println("|        5.                                             |");
	    System.out.println("|        0. Exit                                        |");
	    System.out.println("=========================================================");
	    System.out.println("Enter your choice : ");
	    swValue = scanner.nextInt();

	    // Switch construct
	    switch (swValue) {
	    case 1:
	    	dataSet.testConnection();
	    	break;
	    case 2:
	    	fileBasedInterface.initToReplicateDOList();
	    	if (dataSet.initB2safeConnection()) 
	    		fileBasedInterface.writeReplicaResultToFile(dataSet.replicateAllRequestedDO(fileBasedInterface.initToReplicateDOList()));
	    	break;
	    case 3:
	    	if (dataSet.initB2safeConnection()) dataSet.deleteAllRequestedDO(fileBasedInterface.initToDeleteDOList());
	    	break;
	    case 4:
			for (DataObject dataObject : fileBasedInterface.initToReplicateDOList()) {
				dataObject.toString();
			}
	    	break;
	    case 5:
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
