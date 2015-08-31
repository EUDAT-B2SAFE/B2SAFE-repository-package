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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * This class represents the data objects of a repository or a subset of a repository<br>
 * All those data objects have the same replication target.<br>
 * It is necessary to create an instance of this class to start using rp_core features.<br>
 * It uses the properties contained in the constructor parameter to get the replication parameters<br>
 * <br>
 * The class offers methods to launch the replication actions<br>
 * 
 * @author "S. Coutin (CINES)"
 *
 */
public class DataSet {
	/**
	 * List the properties used as parameter for B2SAFE
	 * @author "S. Coutin (CINES)"
	 *
	 */
	public static enum B2SAFE_CONFIGURATION {
		/**
		 * Resource ID is the ID assigned to the storage resource in the EUDAT RCT tool for the dataset or project.<br>
		 * Will be provided by EUDAT datacentre
		 */
		RESOURCE_ID,
		/**
		 * Transfer protocol to use. Values limited to "irods" for now on
		 */
		B2SAFE_TRANS_PROTOCOL,
		/**
		 * The B2SAFE server URI
		 */
		HOST,
		/**
		 * The port used to connect to server. For example 1247 is the default port for the iRods protocol.
		 */
		PORT,
		/**
		 * User login
		 */
		USER_NAME,
		/**
		 * User password
		 */
		PASSWORD,
		/**
		 * User home directory.
		 */
		HOME_DIRECTORY,
		/**
		 * The irods zone
		 */
		ZONE,
		/**
		 * The iRods resource on which files are stored
		 */
		DEFAULT_STORAGE, 
		/**
		 * This parameter defines the maximum number of parallel transfer threads for the iRods transfer protocol
		 */
		IRODS_TRANSFER_MAX_THREADS
	}
	
	public static enum B2SAFE_TRANS_PROTOCOL_VALUES {
		irods
	}	
	
	protected static Logger log=null;
    protected Properties prop=null;
    private InputStream input =null;
    
    private ReplicationService replicationService = null;

/**
 * Constructor for the DataSet class
 * @param properties
 * 		the properties containing the values for the B2SAFE_CONFIGURATION parameters
 */
    public DataSet(Properties properties){
    	new Log();
    	log= Log.getLogger(DataSet.class.getName());

    	// get the properties from the parameter
    	prop= properties;
    	// check required properties are available
    	if (prop.getProperty(B2SAFE_CONFIGURATION.B2SAFE_TRANS_PROTOCOL.name()) == null) {
    		prop.put("B2SAFE_TRANS_PROTOCOL", "irods");
    		log.info("Property B2SAFE_TRANS_PROTOCOL is missing; setting to irods as default value");
    	}
    	
    	if (replicationService == null){
    		// Instantiate the relevant replication service, based on properties
    		if  (prop.getProperty(B2SAFE_CONFIGURATION.B2SAFE_TRANS_PROTOCOL.name()).trim().equals("irods") )
    			replicationService = new ReplicationServiceIrodsGenericImpl();
    		else
    			log.error("Bad property value B2SAFE_TRANS_PROTOCOL - cannot select the Replication Service");
    	}
    }
	
/**
 * Initialize the connection to EUDAT B2SAFE service<br>
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
	 * Closes all connections
	 * 
	 */
	public void closeConnection() {
		try {
			if (replicationService != null) replicationService.close();
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
		}

	}
	
	/**
	 * Test if the connection is initialized
	 * 
	 * @return 
	 * 		true if connection is initialized,<br>
	 * 		false if not 
	 */
	public boolean isInitialized() {
		return replicationService.isInitialized();
	}
	
	/**
	 * Get some information about the server. This depends upon the protocol.
	 * 
	 * @return
	 * 		List of server properties
	 */
	public Map<String, String> getServerInformation() {
		try {
			return replicationService.getServerInformation();
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
			closeConnection();
			return null;
		} 

	}
	
	public String getServerInformationToString() {
		StringBuilder sb= new StringBuilder();

		try {
			Map<String, String> serverInformation =  replicationService.getServerInformation();

			for (Map.Entry<String, String> entry : serverInformation.entrySet()) {
				sb.append(entry.getKey() + " = " + entry.getValue() + "\r\n");
			}
			return sb.toString();
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
			closeConnection();
			return null;
		} 
	}

	/**
	 * Replication of one data object from the repository to EUDAT B2SAFE<br>
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
		String remoteDirAbsolutePath = null;

    	try {
    		// Copy the input DO values in the output DO
    		operationResult = doToReplicate;
    		// Open the connection
    		if (initB2safeConnection()) {
    			// fill the metadata values in the map
    			metadataInit = new HashMap<String, String>();
    			// ROR is forced to None if it has no value.
    			metadataInit.put("EUDAT/ROR", ( doToReplicate.getRor() != null) ? doToReplicate.getRor() : "None");

    			// Launch replication   			
    			remoteDirAbsolutePath = doToReplicate.getAbsoluteRemoteDirPath(prop.getProperty(B2SAFE_CONFIGURATION.HOME_DIRECTORY.name()).trim());
    			log.info("Launch replication of " + doToReplicate.getLocalFilePath() + " to target directory " + remoteDirAbsolutePath);
    			// Note that operation is a replication
    			operationResult.setOperation("REPLICATE");
    			//get current date time with Date() and note in launch date
    			Date date = new Date();
    			operationResult.setLaunchDate(dateFormat.format(date));
    			// Launch replication
    			replicationService.replicate(doToReplicate.getLocalFilePath(), remoteDirAbsolutePath, metadataInit);
    			// triggers the archive (currently setting ADMIN_Status = "ReadyToArchive")
    			metadataInit.clear();
    			metadataInit.put("ADMIN_Status", "ReadyToArchive");
    			replicationService.modifyMetadataToDataObject(remoteDirAbsolutePath + doToReplicate.getFileName(), metadataInit);

    			// Get feedback
    			metadataFinal = replicationService.getMetadataOfDataObject(remoteDirAbsolutePath + doToReplicate.getFileName());
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

    			log.debug(operationResult.toString());
    			closeConnection();
    		}
    		return operationResult;
    	} catch (ReplicationServiceException ex) {
    		log.error(ex.getMessage());
    		closeConnection();
    		operationResult.setStatus("ERROR");
    		operationResult.setStatusMessage(ex.getMessage());
    		return operationResult;
    	} 
    }

	/**
	 * Replicate a list of data objects from the repository to EUDAT B2SAFE.<br>
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
	 * 		Description of the data object to retrieve, <br>
	 * 		Remote DO is identified by the fileName and the remoteDirPath.<br>
	 * 		The remoteDirPath is relative unless RemoteDirPathIsAbsolute is true<br>
	 * 		The local copy is stored on LocalFilePath directory
	 *
	 */
	public DataObject retrieveOneDOByPath (DataObject dataObject){
		DataObject operationResult = new DataObject();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		File targetDirectory = null;
		String remoteFileAbsolutePath = null;

		try {
			operationResult = dataObject;
			// Note that operation is a retrieval
			operationResult.setOperation("RETRIEVE");
			//get current date time with Date() and note in launch date
			Date date = new Date();
			operationResult.setLaunchDate(dateFormat.format(date));

			if (dataObject.getRemoteDirPathIsAbsolute())
				remoteFileAbsolutePath = dataObject.getRemoteDirPath()+dataObject.getFileName();
			else
				remoteFileAbsolutePath = prop.getProperty(B2SAFE_CONFIGURATION.HOME_DIRECTORY.name()).trim() + dataObject.getRemoteDirPath()+dataObject.getFileName();
			
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
	 * FOR TEST PURPOSE ONLY<br>
	 * This method deletes a data object in EUDAT B2SAFE<br>
	 * It must be used carefully as it can create a discrepancy between repository and replication<br>
	 * <br>
	 * On the B2SAFE side, a trigger manages the deletion of first replica PID record<br>
	 * 
	 * @param dataObject
	 * 		in this release, the DO to delete is identified by the fileName and the remoteDirPath.<br> 
	 * 		The remoteDirPath is relative unless RemoteDirPathIsAbsolute is true
	 */
	public DataObject deleteDO(DataObject dataObject) {
		DataObject operationResult = new DataObject();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String fileAbsolutePath = null;

		try {
			operationResult = dataObject;
			// Note that operation is a deletion
			operationResult.setOperation("DELETE");
			// get current date time with Date() and note in launch date
			Date date = new Date();
			operationResult.setLaunchDate(dateFormat.format(date));

			if (dataObject.getRemoteDirPathIsAbsolute())
				fileAbsolutePath = dataObject.getRemoteDirPath()+dataObject.getFileName();
			else
				fileAbsolutePath = prop.getProperty(B2SAFE_CONFIGURATION.HOME_DIRECTORY.name()).trim() + dataObject.getRemoteDirPath()+dataObject.getFileName();
			
			// Launch deletion
			log.info("Delete " + fileAbsolutePath);
			// It is necessary to force the delete,
			// The delete methods returns a boolean value of true if file exists, false if not
			if (replicationService.delete(fileAbsolutePath, true)) {
				operationResult.setStatus("SUCCESS");
			}
			else
			{
				operationResult.setStatus("ERROR");
				operationResult.setStatusMessage("File doesn't exist");
			}
			// get current date time with Date() and note in end date
			date = new Date();
			operationResult.setEndDate(dateFormat.format(date));
			return operationResult;
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
			operationResult.setStatus("ERROR");
			operationResult.setStatusMessage(ex.getMessage());
			return operationResult;
		}
	}

	/**
	 * FOR TEST PURPOSE ONLY<br>
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
	 * List the content of a given directory in B2SAFE, with the ability to use absolute or relative path
	 * 
	 * @param remoteDirectoryPath
	 * 		Remote directory path. If relative, the base path is the HOME_DIRECTORY property
	 * @param isAbsolute
	 * 		true if the path is absolute, false if relative
	 */
	public List<DataObject> listDOFromDirectory (String remoteDirectoryPath, boolean isAbsolute){
		String remoteDirectoryAbsolutePath = null;
		
		remoteDirectoryAbsolutePath = (isAbsolute ? remoteDirectoryPath : prop.getProperty(B2SAFE_CONFIGURATION.HOME_DIRECTORY.name()).trim() + remoteDirectoryPath);
		return listDOFromDirectory(remoteDirectoryAbsolutePath);
	}

	/**
	 * List the content of a given directory in B2SAFE.
	 * 
	 * @param remoteDirectoryPath
	 * 		remote directory absolute path.
	 */
	public List<DataObject> listDOFromDirectory (String remoteDirectoryAbsolutePath){
		List<DataObject> doList = new ArrayList<DataObject>();
		List<String> doFullPathList = new ArrayList<String>();
		Map<String, AVUMetaData> mdList = new HashMap<String, AVUMetaData>();
		
		try {
			log.debug("List content of absolute directory ["+ remoteDirectoryAbsolutePath + "]");
			doFullPathList = replicationService.list(remoteDirectoryAbsolutePath, true);
			// Loop all the results
			for (String doName:doFullPathList) {
				DataObject tmpDO = new DataObject();
				
	            int lastSlash = doName.lastIndexOf("/");
	            String dirName = doName.substring(0,lastSlash);
	            String fileName = doName.substring(lastSlash+1);
				
				// set the DO information
				tmpDO.setRemoteDirPath(dirName);
				tmpDO.setFileName(fileName);
				tmpDO.setRemoteDirPathIsAbsolute(true);
				// set the DO metadata
				tmpDO.setEudatMetadata(replicationService.getMetadataOfDataObject(doName));
					            
				// add DO to the list
				doList.add(tmpDO);
			}
		
			return doList;
			
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
			return doList;
		} 
	}

	/**
	 * Read the metadata for a given data object
	 * @param dataObject
	 * 		DataObject identifying the file by the fileName and the remoteDirPath.<br> 
	 * 		The remoteDirPath is relative unless RemoteDirPathIsAbsolute is true<br>
	 * @return
	 * 		A DataObject updated with eudatMetadata populated from B2SAFE.<br>
	 * 		If the operation is successful, DataObject is set to "SUCCESS" <br>
	 * 		If the operation has failed, DataObject is set to "ERROR" 
	 */
	public DataObject getMetadataFromOneDOByPath (DataObject dataObject){
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		File targetDirectory = null;
		String remoteFileAbsolutePath = null;

		try {
			// Note that operation is a matadata retrieval
			dataObject.setOperation("MD_READ");
			//get current date time with Date() and note in launch date
			Date date = new Date();
			dataObject.setLaunchDate(dateFormat.format(date));

			if (dataObject.getRemoteDirPathIsAbsolute())
				remoteFileAbsolutePath = dataObject.getRemoteDirPath()+dataObject.getFileName();
			else
				remoteFileAbsolutePath = prop.getProperty(B2SAFE_CONFIGURATION.HOME_DIRECTORY.name()).trim() + dataObject.getRemoteDirPath()+dataObject.getFileName();
			
			// Launch Retrieval
			log.info("Retrieve " + remoteFileAbsolutePath + " Metadata ");
			dataObject.setEudatMetadata(replicationService.getMetadataOfDataObject(remoteFileAbsolutePath));
			
			//get current date time with Date() and note in end date
			date = new Date();
			dataObject.setEndDate(dateFormat.format(date));
			dataObject.setStatus("SUCCESS");
			
			return dataObject;
			
		} catch (ReplicationServiceException ex) {
			log.error(ex.getMessage());
			dataObject.setStatus("ERROR");
			dataObject.setStatusMessage(ex.getMessage());
			return dataObject;
		} 
		
	}
	
	
}