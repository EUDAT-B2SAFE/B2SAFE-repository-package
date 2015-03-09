/**
 * Institute of Formal and Applied Linguistics
 * Charles University in Prague, Czech Republic
 * 
 * http://ufal.mff.cuni.cz
 * 
 */

package fr.cines.eudat.repopack.b2safe_rp_core;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Interface describing a ReplicationService
 * 
 * @author Amir Kamran
 * 
 */
abstract class ReplicationService {

	/**
	 * Initialize the ReplicationService with the provided configuration and try
	 * to establish a connection with the replication server
	 * 
	 * @param config
	 *            required configuration for replication server connection
	 * @return connection status
	 * @throws ReplicationServiceException
	 */
	abstract boolean initialize(Properties config) throws ReplicationServiceException;

	/**
	 * Returns whether the service is already initialized
	 * 
	 * @return true (initialized) or false (not initialized)
	 */
	abstract boolean isInitialized();

	/**
	 * Replicate a local resource (file or folder) to the replication server
	 * 
	 * @param localFileName
	 *            absolute path of the local resource
	 * @throws ReplicationServiceException
	 */
	abstract void replicate(String localFileName) throws ReplicationServiceException;

	/**
	 * Replicate a local resource (file or folder) to the replication server
	 * 
	 * @param localFileName
	 *            absolute path of the local resource
	 * @param metadata
	 *            key value pairs of metadata
	 * @throws ReplicationServiceException
	 */
	abstract void replicate(String localFileName, Map<String, String> metadata)
			throws ReplicationServiceException;

	/**
	 * Replicate a local resource (file or folder) to the replication server
	 * 
	 * @param localFileName
	 *            absolute path of the local resource
	 * @param metadata
	 *            key value pairs of metadata
	 * @param force
	 *            whether to overwrite if file already exists
	 * @throws ReplicationServiceException
	 */
	abstract void replicate(String localFileName, Map<String, String> metadata,
			boolean force) throws ReplicationServiceException;

	/**
	 * Replicate a local resource (file or folder) to the replication server
	 * 
	 * @param localFileName
	 *            absolute path of the local resource
	 * @param remoteDirectory
	 *            the folder where the file should be copied on replication
	 *            server
	 * @param metadata
	 *            key value pairs of metadata
	 * @throws ReplicationServiceException
	 */
	abstract void replicate(String localFileName, String remoteDirectory,
			Map<String, String> metadata) throws ReplicationServiceException;

	/**
	 * Replicate a local resource (file or folder) to the replication server
	 * 
	 * @param localFileName
	 *            absolute path of the local resource
	 * @param remoteDirectory
	 *            the folder where the file should be copied on replication
	 *            server
	 * @param metadata
	 *            key value pairs of metadata
	 * @param force
	 *            whether to overwrite if file already exists
	 * @throws ReplicationServiceException
	 */
	abstract void replicate(String localFileName, String remoteDirectory,
			Map<String, String> metadata, boolean force)
			throws ReplicationServiceException;

	/**
	 * Delete a resource from the replication server
	 * 
	 * @param path
	 *            absolute path of the resource
	 * @return
	 * @throws ReplicationServiceException
	 */
	abstract boolean delete(String path) throws ReplicationServiceException;

	/**
	 * Delete a resource from the replication server
	 * 
	 * @param path
	 *            absolute path of the resource
	 * @param force
	 *            forcefully delete the resource
	 * @return
	 * @throws ReplicationServiceException
	 */
	abstract boolean delete(String path, boolean force)
			throws ReplicationServiceException;

	/**
	 * retrieve a resource from replication server and copy to the specified
	 * location
	 * 
	 * @param remoteFileName
	 *            absolute path of the remote resource
	 * @param localFileName
	 *            absolute destination path
	 * @throws ReplicationServiceException
	 */
	abstract void retrieveFile(String remoteFileName, String localFileName)
			throws ReplicationServiceException;

	/**
	 * add metadata to a file on replication server
	 * 
	 * @param filePath
	 *            absolute path of the file
	 * @param metadata
	 *            key value pairs of metadata
	 * @throws ReplicationServiceException
	 */
	abstract void addMetadataToDataObject(String filePath, Map<String, String> metadata)
			throws ReplicationServiceException;

	/**
	 * modify metadata of a file on replication server
	 * 
	 * @param filePath
	 *            absolute path of the file
	 * @param metadata
	 *            key value pairs of metadata
	 * @throws ReplicationServiceException
	 */
	abstract void modifyMetadataToDataObject(String filePath,
			Map<String, String> metadata) throws ReplicationServiceException;

	/**
	 * add metadata to a folder on replication server
	 * 
	 * @param collecitonPath
	 *            absolute path of the remote folder
	 * @param metadata
	 *            key value paris of metadata
	 * @throws ReplicationServiceException
	 */
	abstract void addMetadataToCollection(String collecitonPath,
			Map<String, String> metadata) throws ReplicationServiceException;

	/**
	 * modify metadata of a folder on replication server
	 * 
	 * @param collectionPath
	 *            absolute path of the remote folder
	 * @param metadata
	 *            key value paris of metadata
	 * @throws ReplicationServiceException
	 */
	abstract void modifyMetadataToCollection(String collectionPath,
			Map<String, String> metadata) throws ReplicationServiceException;

	/**
	 * @param dataObjectAbsolutePath
	 * @return
	 * @throws ReplicationServiceException
	 */
	abstract Map<String, AVUMetaData> getMetadataOfDataObject(String dataObjectAbsolutePath)
			throws ReplicationServiceException;

	/**
	 * list resources stored on the default location
	 * 
	 * @return
	 * @throws ReplicationServiceException
	 */
	abstract List<String> list() throws ReplicationServiceException;

	/**
	 * list resources stored on the default location
	 * 
	 * @param returnAbsPath
	 *            whether to return the full path of the resources
	 * @return
	 * @throws ReplicationServiceException
	 */
	abstract List<String> list(boolean returnAbsPath) throws ReplicationServiceException;

	/**
	 * list resources stored on the specified location
	 * 
	 * @param remoteDirectory
	 *            remote location
	 * @param returnAbsPath
	 *            whether to return the full path of the resources
	 * @return
	 * @throws ReplicationServiceException
	 */
	abstract List<String> list(String remoteDirectory, boolean returnAbsPath)
			throws ReplicationServiceException;

	/**
	 * Query for resources on replication server based on provided metadata
	 * criteria
	 * 
	 * @param metadata
	 * @return
	 * @throws ReplicationServiceException
	 */
	abstract List<String> search(Map<String, String> metadata)
			throws ReplicationServiceException;
	
	/**
	 * Get some information about the server. This depends upon the protocol.
	 * 
	 * @return
	 * 		List of server properties
	 */
	abstract Map<String, String> getServerInformation();


	/**
	 * close all the open connections and resources
	 * 
	 * @throws ReplicationServiceException
	 */
	abstract void close() throws ReplicationServiceException;

}
