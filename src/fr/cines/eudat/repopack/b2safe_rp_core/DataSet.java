package fr.cines.eudat.repopack.b2safe_rp_core;

import fr.cines.eudat.log.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;

import org.apache.log4j.Logger;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.utils.LocalFileUtils;

/**
 * This class represents the data objects of a repository or a subset of a repository
 * All those data objects have the same replication target.
 * It is necessary to create an instance of this class to start using rp_core features.
 * It uses the properties from the config.properties file to get the replication parameters
 * 
 * The class offers methods to launch the replication actions
 * 
 * @author "S. Coutin (CINES)"
 *
 */
public class DataSet {
	// public Object name;
	protected static Logger log=null;
    private Properties prop=null;
    private InputStream input =null;
    
    private ReplicationService replicationService = null;

/**
 * Constructor for the DataSet class
 */
    public DataSet(){
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
    	// TODO test the other values of the properties

//    	if  (prop.getProperty("B2SAFE_TRANS_PROTOCOL").equals("irods") )
    		replicationService = new ReplicationServiceIrodsGenericImpl();
//    	else
//    		log.error("Bad property value B2SAFE_TRANS_PROTOCOL - cannot select the Replication Service");
    }
	
/**
 * Initialize the connection to EUDAT B2SAFE service
 * Must be done before launching any action
 * 
 * @return 
 * 		true if connection is successful,
 * 		false if not 
 */
    public boolean initB2safeConnection() {
		try {
			return replicationService.initialize(prop);
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
			return false;
		} 

	}

    /**
     * To remove
     */
    public String testConnection() {
    	if (replicationService.isInitialized()) {
    		return "Already connected to B2SAFE ";
    	}
    	else {
    		if (this.initB2safeConnection())
    			return "Successful connection to B2SAFE ";
    		else
    			return "Error connecting to B2SAFE ";
    	}
    }
	
	/**
	 * Replication of one data object from the repository to EUDAT B2SAFE
	 * @param doToReplicate
	 * 		Description of the data object to replicate in B2SAFE
	 * @return 
	 * 		Description of the data object with additional information resulting from the replication
	 */
    public DataObject replicateOneDO(DataObject doToReplicate) {
    	Map<String, String> metadataInit = null;
    	Map<String, AVUMetaData> metadataFinal = null;
    	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	DataObject operationResult = new DataObject();

    	try {
    		// TODO use the metadata map from the DO - Requires to change types even in the ReplicationService class
    		// fill the metadata values in the map
    		metadataInit = new HashMap<String, String>();
    		// ROR is forced to None if it has no value.
    		metadataInit.put("EUDAT/ROR", ( doToReplicate.getRor() != null) ? doToReplicate.getRor() : "None");
    		metadataInit.put("resource_id", prop.getProperty("RESOURCE_ID"));
    		// Copy the input DO values in the output DO
    		operationResult = doToReplicate;

    		// Launch replication
    		log.info("Launch replication of " + doToReplicate.getLocalFilePath() + " to target collection " + doToReplicate.getRemoteDirPath());
    		// Note that operation is a replication
    		operationResult.setOperation("REPLICATE");
    		//get current date time with Date() and note in launch date
    		Date date = new Date();
    		operationResult.setLaunchDate(dateFormat.format(date));
    		// Launch replication
    		replicationService.replicate(doToReplicate.getLocalFilePath(), doToReplicate.getRemoteDirPath(), metadataInit);
    		// triggers the archive (currently setting ADMIN_Status = "ReadyToArchive")
    		metadataInit.clear();
    		metadataInit.put("ADMIN_Status", "ReadyToArchive");
    		replicationService.modifyMetadataToDataObject(prop.getProperty("HOME_DIRECTORY") + doToReplicate.getRemoteDirPath() + doToReplicate.getFileName(), metadataInit);

    		// Get feedback
    		metadataFinal = replicationService.getMetadataOfDataObject(prop.getProperty("HOME_DIRECTORY") + doToReplicate.getRemoteDirPath()+doToReplicate.getFileName());
    		for (Map.Entry<String, AVUMetaData> entry1 : metadataFinal.entrySet()) {
    			if (entry1.getKey().equalsIgnoreCase("ADMIN_Status")) operationResult.setStatusMessage(entry1.getValue().getValue());
    			//if (entry1.getKey().equalsIgnoreCase("INFO_TimeOfTransfer")) replicaResult.setEndDate(entry1.getValue().getValue());
    			if (entry1.getKey().equalsIgnoreCase("OTHER_original_checksum")) operationResult.setChecksum(entry1.getValue().getValue());
    			if (entry1.getKey().equalsIgnoreCase("PID")) operationResult.setEudatPid(entry1.getValue().getValue());
    		}
    		operationResult.setEudatMetadata(metadataFinal);
    		//get current date time with Date() and note in end date
    		date = new Date();
    		operationResult.setEndDate(dateFormat.format(date));
    		operationResult.setStatus((operationResult.getStatusMessage().equals("Archive_ok") ) ? "SUCCESS" : "ERROR");


    		// TODO remove or write to log after development
    		System.out.println(operationResult.toString());
    		return operationResult;
    	} catch (ReplicationServiceException ex) {
    		log.error(ex.getMessage());
    		operationResult.setStatus("ERROR");
    		operationResult.setStatusMessage(ex.getMessage());
    		return operationResult;
    	} 
    }

	/**
	 * Replicate a list of data objects from the repository to EUDAT B2SAFE.
	 * It tries to replicate each of the list data objects, even if one replication fails
	 * 
	 * @see replicateOneDO
	 * @param listDOToReplicate
	 * @return
	 * 		The list of data objects with the replication result additional information
	 */
	public ArrayList<DataObject> replicateAllRequestedDO(ArrayList<DataObject> listDOToReplicate) {
		ArrayList<DataObject> replicaResult = new ArrayList<DataObject>();

		for (DataObject dataObject : listDOToReplicate) {
			replicaResult.add(replicateOneDO(dataObject));
		}
		return replicaResult;
	}

	/**
	 * Retrieve a data object (ie a file) from B2SAFE and store it in the repository
	 * 
	 * @param dataObject
	 * 		Description of the data object to retrieve, full path on B2SAFE must be provided
	 */
	public DataObject retrieveOneDOByPath (DataObject dataObject){
		DataObject operationResult = new DataObject();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		File targetDirectory = null;

		try {
			operationResult = dataObject;
			// Note that operation is a retrieval
			operationResult.setOperation("RETRIEVE");
			//get current date time with Date() and note in launch date
			Date date = new Date();
			operationResult.setLaunchDate(dateFormat.format(date));

			String remoteFileAbsolutePath = prop.getProperty("HOME_DIRECTORY") + dataObject.getRemoteDirPath()+dataObject.getFileName();
			
			// create local directory if it doesn't exist
			targetDirectory = new File(dataObject.getLocalFilePath());
			if (!targetDirectory.isDirectory()) targetDirectory.mkdirs();
			
			// Launch Retrieval
			log.info("Retrieve " + remoteFileAbsolutePath + " TO " + dataObject.getLocalFilePath());
			replicationService.retrieveFile(remoteFileAbsolutePath, dataObject.getLocalFilePath());
			
			//get current date time with Date() and note in end date
			date = new Date();
			operationResult.setEndDate(dateFormat.format(date));
			operationResult.setStatus("SUCCESS");
			
			return operationResult;
			
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
			operationResult.setStatus("ERROR");
			operationResult.setStatusMessage(ex.getMessage());
			return operationResult;
		} 
	}
	
	/**
	 * Retrieve a list of data object from B2SAFE and store them in the repository
	 * 
	 * @param listDOToRetrieve
	 * 		List of data objects to retrieve
	 */
	public ArrayList<DataObject> retrieveListOfDOByPath(ArrayList<DataObject> listDOToRetrieve) {
		ArrayList<DataObject> operationResult = new ArrayList<DataObject>();

		for (DataObject dataObject : listDOToRetrieve) {
			operationResult.add(retrieveOneDOByPath(dataObject));
		}
		return operationResult;
	}
	
	/**
	 * FOR TEST PURPOSE ONLY
	 * This method deletes a data object in EUDAT B2SAFE
	 * It must be used carefully as it can create a discrepancy between repository and replication
	 * 
	 * On the B2SAFE side, a trigger manages the deletion of first replica PID record
	 * 
	 * @param dataObject
	 * 		in this release, the DO to delete is identified by the fileName and the remoteDirPath
	 */
	public DataObject deleteDO(DataObject dataObject) {
		DataObject operationResult = new DataObject();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

		try {
			operationResult = dataObject;
			// Note that operation is a deletion
			operationResult.setOperation("DELETE");
			//get current date time with Date() and note in launch date
			Date date = new Date();
			operationResult.setLaunchDate(dateFormat.format(date));

			String fileAbsolutePath = prop.getProperty("HOME_DIRECTORY") + dataObject.getRemoteDirPath()+dataObject.getFileName();
			// Launch deletion
			log.info("Delete " + fileAbsolutePath);
			// It is necessary to force the delete
			// TODO implement on the iRods connector once an interface will be created
			replicationService.delete(fileAbsolutePath, true);
			
			//get current date time with Date() and note in end date
			date = new Date();
			operationResult.setEndDate(dateFormat.format(date));
			operationResult.setStatus("SUCCESS");
			return operationResult;
	} catch (ReplicationServiceException ex) {
		log.error(ex.getMessage());
		operationResult.setStatus("ERROR");
		operationResult.setStatusMessage(ex.getMessage());
		return operationResult;
	} 
	}

	/**
	 * FOR TEST PURPOSE ONLY
	 * Deletes a list of data objects
	 * 
	 * @see deleteDO
	 * @param listDOToDelete
	 */
	public ArrayList<DataObject> deleteAllRequestedDO(ArrayList<DataObject> listDOToDelete) {
		ArrayList<DataObject> operationResult = new ArrayList<DataObject>();

		for (DataObject dataObject : listDOToDelete) {
			operationResult.add(deleteDO(dataObject));
		}
		return operationResult;
	}

	/**
	 * Closes all connections
	 * 
	 */
	public void close() {
		try {
			replicationService.close();
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
		}

	}

}