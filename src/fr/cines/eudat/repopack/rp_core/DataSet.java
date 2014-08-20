package fr.cines.eudat.repopack.rp_core;

import fr.cines.eudat.log.Log;

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
		// TODO Might not be the right place to create the object...
		replicationService = new ReplicationService();
	}
	
/**
 * Initialize the connection to EUDAT B2SAFE service
 * Must be done before launching any action
 * 
 * @return 
 * 		true if connection is successful,
 * 		false if not 
 */
    public boolean initB2safeConnection(){
		try {
			return replicationService.initialize(prop).isSuccessful();
		} catch (JargonException ex) {
			log.error(ex.getMessage());
			return false;
		}
	}

    /**
     * To remove
     */
	public void testConnection() {
		log.info("Begin method DataSet.TestConnection");
		if (this.initB2safeConnection())
			log.info("Successful connection to B2SAFE ");
		else
			log.info("Error connecting to B2SAFE ");
		log.debug("End method DataSet.TestConnection");
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
		DataObject replicaResult = new DataObject();

		try {
				// TODO use the metadata map from the DO - Requires to change types even in the ReplicationService class
				// fill the metadata values in the map
				metadataInit = new HashMap<String, String>();
				// TODO we need to sync up metadata name with the one expected by ingestion rule. So far it is EUDAT_ROR
				metadataInit.put("ROR", doToReplicate.getRor());

				// Launch replication
				log.info("Launch replication of " + doToReplicate.getLocalFilePath() + " to target collection " + doToReplicate.getRemoteDirPath());
				//get current date time with Date()
				Date date = new Date();
				doToReplicate.setReplicaLaunchDate(dateFormat.format(date));
				replicationService.replicate(doToReplicate.getLocalFilePath(), doToReplicate.getRemoteDirPath(), metadataInit);

				// Get feedback
				replicaResult = doToReplicate;
				metadataFinal = replicationService.getMetadataOfDataObject(prop.getProperty("HOME_DIRECTORY") + doToReplicate.getRemoteDirPath()+doToReplicate.getFileName());
				for (Map.Entry<String, AVUMetaData> entry1 : metadataFinal.entrySet()) {
					if (entry1.getKey().equalsIgnoreCase("ADMIN_Status")) replicaResult.setAdminStatus(entry1.getValue().getValue());
					if (entry1.getKey().equalsIgnoreCase("INFO_TimeOfTransfer")) replicaResult.setReplicaSuccessDate(entry1.getValue().getValue());
					if (entry1.getKey().equalsIgnoreCase("OTHER_original_checksum")) replicaResult.setChecksum(entry1.getValue().getValue());
					if (entry1.getKey().equalsIgnoreCase("PID")) replicaResult.setEudatPid(entry1.getValue().getValue());
				}
				replicaResult.setEudatMetadata(metadataFinal);
				// TODO remove or write to log after development
				System.out.println(replicaResult.toString());
				return replicaResult;
		} catch (JargonException ex) {
			log.error(ex.getMessage());
			return replicaResult;
		} catch (IOException ex) {
			log.error(ex.getMessage());
			return replicaResult;
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
	 * FOR TEST PURPOSE ONLY
	 * This method deletes a data object (ie a file) in EUDAT B2SAFE
	 * It must be used carefully as it can create a discrepancy between repository and replication
	 * 
	 * On the B2SAFE side, a trigger manages the deletion of first replica PID record
	 * 
	 * @param dataObject
	 * 		in this release, the DO to delete is identified by the fileName and the remoteDirPath
	 */
	public void deleteDO(DataObject dataObject) {
		try {
			String fileAbsolutePath = prop.getProperty("HOME_DIRECTORY") + dataObject.getRemoteDirPath()+dataObject.getFileName();
			// Launch deletion
			log.info("Delete " + fileAbsolutePath);
			// It is necessary to force the delete
			// TODO implement on the iRods connector once an interface will be created
			replicationService.delete(fileAbsolutePath, true);
		} catch (JargonException ex) {
			log.error(ex.getMessage());
		}		
	}

	/**
	 * FOR TEST PURPOSE ONLY
	 * Deletes a list of data objects
	 * 
	 * @see deleteDO
	 * @param listDOToDelete
	 */
	public void deleteAllRequestedDO(ArrayList<DataObject> listDOToDelete) {

		for (DataObject dataObject : listDOToDelete) {
			deleteDO(dataObject);
		}
	}
	
}