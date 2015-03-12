package fr.cines.eudat.repopack.b2safe_rp_core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.connection.IRODSServerProperties;
import org.irods.jargon.core.connection.IRODSSession;
import org.irods.jargon.core.connection.SettableJargonProperties;
import org.irods.jargon.core.connection.auth.AuthResponse;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.exception.JargonRuntimeException;
import org.irods.jargon.core.packinstr.TransferOptions;
import org.irods.jargon.core.pub.CollectionAO;
import org.irods.jargon.core.pub.DataObjectAO;
import org.irods.jargon.core.pub.DataTransferOperations;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.domain.AvuData;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.query.AVUQueryElement;
import org.irods.jargon.core.query.AVUQueryElement.AVUQueryPart;
import org.irods.jargon.core.query.AVUQueryOperatorEnum;
import org.irods.jargon.core.query.JargonQueryException;
import org.irods.jargon.core.query.MetaDataAndDomainData;
import org.irods.jargon.core.transfer.TransferControlBlock;
import org.irods.jargon.core.utils.LocalFileUtils;

import fr.cines.eudat.repopack.b2safe_rp_core.DataSet.B2SAFE_CONFIGURATION;



class ReplicationServiceIrodsGenericImpl extends ReplicationService {
	
	IRODSFileSystem irodsFileSystem = null;
	IRODSAccount irodsAccount = null;
	Properties configuration = null;
	

	protected boolean initialize(Properties config) throws ReplicationServiceException {

    	configuration = config;
		String host = config.getProperty(B2SAFE_CONFIGURATION.HOST.name()).trim();
    	String port = config.getProperty(B2SAFE_CONFIGURATION.PORT.name()).trim();
    	String username = config.getProperty(B2SAFE_CONFIGURATION.USER_NAME.name()).trim();
    	String pass = config.getProperty(B2SAFE_CONFIGURATION.PASSWORD.name()).trim();
    	String homedir = config.getProperty(B2SAFE_CONFIGURATION.HOME_DIRECTORY.name()).trim();
    	if ( !homedir.endsWith("/") ) homedir += "/";
    	String zone = config.getProperty(B2SAFE_CONFIGURATION.ZONE.name()).trim();
    	String default_storage = config.getProperty(B2SAFE_CONFIGURATION.DEFAULT_STORAGE.name()).trim();
    	    	
		try {
			irodsAccount = IRODSAccount.instance(host, Integer.valueOf(port),
					username, pass, homedir, zone, default_storage);

			irodsFileSystem = IRODSFileSystem.instance();

			AuthResponse response = irodsFileSystem.getIRODSAccessObjectFactory().authenticateIRODSAccount(irodsAccount);
			overrideJargonProperties(getSettableJargonProperties());

			return response.isSuccessful();
		} catch (NumberFormatException e) {
			// TODO manage logging
			//log.error("Invalid value of port: " + port, e);
			return false;
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}
	
	protected boolean isInitialized() {
		return irodsFileSystem != null;
	}
	
	protected void replicate(String localFileName)
			throws ReplicationServiceException {
		replicate(localFileName, null, false);
	}

	protected void replicate(String localFileName, Map<String, String> metadata)
			throws ReplicationServiceException {
		replicate(localFileName, metadata, false);
	}

	protected void replicate(String localFileName, Map<String, String> metadata,
			boolean force) throws ReplicationServiceException {
		String defaultRemoteLocation = configuration
				.getProperty(B2SAFE_CONFIGURATION.DEFAULT_STORAGE.name()).trim();
		replicate(localFileName, defaultRemoteLocation, metadata);
	}

	protected void replicate(String localFileName, String remoteDirectory,
			Map<String, String> metadata) throws ReplicationServiceException {
		replicate(localFileName, remoteDirectory, metadata, false);
	}

	protected void replicate(String localFileName, String remoteDirectory, Map<String, String> metadata, boolean force) 
			throws ReplicationServiceException {

		try {
			if(overrideJargonProperties!=null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(overrideJargonProperties);
			}

			// Calculate local file related metadata. This checks also that the local file exists
			File localFile = new File(localFileName);    				
			if(metadata==null) {
				metadata = new HashMap<String, String>();
			}
			metadata.put("OTHER_original_filesize", String.valueOf(localFile.length()));
			metadata.put("OTHER_original_checksum", LocalFileUtils.digestByteArrayToString(LocalFileUtils.computeMD5FileCheckSumViaAbsolutePath(localFile.getAbsolutePath())));

			// Handle the data transfer operation
			IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);

			IRODSFile targetDirectory = irodsFileFactory.instanceIRODSFile(irodsAccount.getHomeDirectory() + remoteDirectory);

			if(!targetDirectory.exists()) {
				// SCn : changing to mkdirs as this deals with multiple levels collections
				targetDirectory.mkdirs();
				// Set the resource_id metadata for the new collection
				Map<String, String> collMetadata = new HashMap<String, String>();
				collMetadata.put("resource_id", configuration.getProperty(B2SAFE_CONFIGURATION.RESOURCE_ID.name()).trim());
				addMetadataToCollection(targetDirectory.getAbsolutePath(), collMetadata);
			}

			String targetFile = targetDirectory.getCanonicalPath() + IRODSFile.PATH_SEPARATOR + localFile.getName();

			IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(targetFile);

			DataTransferOperations dataTransferOperations = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getDataTransferOperations(irodsAccount);

			TransferControlBlock controlBlock = irodsFileSystem
					.getIrodsSession()
					.buildDefaultTransferControlBlockBasedOnJargonProperties();
			
			controlBlock.resetTransferData();
			if(force) {
				controlBlock.getTransferOptions().setForceOption(TransferOptions.ForceOption.USE_FORCE);    		
			}
			// controlBlock.getTransferOptions().setMaxThreads(32);
			// controlBlock.getTransferOptions().setComputeChecksumAfterTransfer(true);
			long startTime = System.currentTimeMillis();

			dataTransferOperations.putOperation(localFile, remoteFile, null, controlBlock);

			long endTime   = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			// logTransferInfo(controlBlock, totalTime);

			// Write remote file metadata
			if(localFile.isDirectory()) {
				addMetadataToCollection(remoteFile.getAbsolutePath(), metadata);
			} else {
				addMetadataToDataObject(remoteFile.getAbsolutePath(), metadata);
			}
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		} catch (IOException e) {
			throw new ReplicationServiceException(e);
		}
	}
	
	void logTransferInfo(TransferControlBlock tcb, long totalTime) {
		System.out.println("Transfer duration = " + totalTime + " millisecs");
		System.out.println("getTotalBytesToTransfer = " + tcb.getTotalBytesToTransfer());
		System.out.println("getTotalFilesTransferredSoFar = " + tcb.getTotalFilesTransferredSoFar());
		System.out.println("getTransferOptions = " + tcb.getTransferOptions().toString());
	}
	
	
	
	protected boolean delete(String path) throws ReplicationServiceException {
		return delete(path, false);
	}

	protected boolean delete(String path, boolean force)
			throws ReplicationServiceException {
		try {

			if (overrideJargonProperties != null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(
						overrideJargonProperties);
			}

			IRODSFileFactory irodsFileFactory = irodsFileSystem
					.getIRODSFileFactory(irodsAccount);
			IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(path);
			if (remoteFile.exists()) {
				if (force) {
					return remoteFile.deleteWithForceOption();
				} else {
					return remoteFile.delete();
				}
			}
			else {
				return (false);
			}
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		} catch (JargonRuntimeException e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected void retrieveFile(String remoteFileName, String localFileName) 
			throws ReplicationServiceException {
		try {
			if(overrideJargonProperties!=null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(overrideJargonProperties);
			}		
			File localFile = new File(localFileName);

			IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);

			IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(remoteFileName);

			DataTransferOperations dataTransferOperations = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getDataTransferOperations(irodsAccount);

			dataTransferOperations.getOperation(remoteFile.getAbsolutePath(), localFile.getAbsolutePath(), "", null, null);    	
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected void addMetadataToDataObject(String filePath,
			Map<String, String> metadata) throws ReplicationServiceException {
		try {
			DataObjectAO dataObjectAO = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getDataObjectAO(irodsAccount);
			for (Map.Entry<String, String> md : metadata.entrySet()) {
				AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
				dataObjectAO.addAVUMetadata(filePath, data);
			}
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}
   
	protected void modifyMetadataToDataObject(String filePath,
			Map<String, String> metadata) throws ReplicationServiceException {
		try {
			DataObjectAO dataObjectAO = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getDataObjectAO(irodsAccount);
			for (Map.Entry<String, String> md : metadata.entrySet()) {
				AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
				dataObjectAO.modifyAvuValueBasedOnGivenAttributeAndUnit(
						filePath, data);
			}
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected void addMetadataToCollection(String collectionPath,
			Map<String, String> metadata) throws ReplicationServiceException {
		try {
			CollectionAO collectionAO = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getCollectionAO(irodsAccount);
			for (Map.Entry<String, String> md : metadata.entrySet()) {
				AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
				collectionAO.addAVUMetadata(collectionPath, data);
			}
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected void modifyMetadataToCollection(String collectionPath,
			Map<String, String> metadata) throws ReplicationServiceException {
		try {
			CollectionAO collectionAO = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getCollectionAO(irodsAccount);
			for (Map.Entry<String, String> md : metadata.entrySet()) {
				AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
				collectionAO.modifyAvuValueBasedOnGivenAttributeAndUnit(
						collectionPath, data);
			}
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected Map<String, AVUMetaData> getMetadataOfDataObject(String dataObjectAbsolutePath) 
			throws ReplicationServiceException {

		try {
			Map<String, AVUMetaData> eudatMetadata = new HashMap<String, AVUMetaData>();
			DataObjectAO dataObjectAO = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);

			List<MetaDataAndDomainData> listMetaData = dataObjectAO.findMetadataValuesForDataObject(dataObjectAbsolutePath);
			for (MetaDataAndDomainData temp : listMetaData) {
				eudatMetadata.put(temp.getAvuAttribute(), new AVUMetaData(temp.getAvuAttribute(),temp.getAvuValue(),temp.getAvuUnit()));
			}

			return eudatMetadata;
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}

	}
    
	protected List<String> list() throws ReplicationServiceException {
		return list(false);
	}

	protected List<String> list(boolean returnAbsPath)
			throws ReplicationServiceException {
		String defaultRemoteLocation = configuration
				.getProperty(B2SAFE_CONFIGURATION.DEFAULT_STORAGE.name()).trim();
		return list(defaultRemoteLocation, returnAbsPath);
	}

	protected List<String> list(String remoteDirectory, boolean returnAbsPath)
			throws ReplicationServiceException {
		try {

			if (overrideJargonProperties != null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(
						overrideJargonProperties);
			}

			IRODSFileFactory irodsFileFactory = irodsFileSystem.getIRODSFileFactory(irodsAccount);
			IRODSFile irodsDirectory = irodsFileFactory.instanceIRODSFile(remoteDirectory);

			String[] list = irodsDirectory.list();
			List<String> retList = new ArrayList<String>();
			for (String l : list) {
				if (returnAbsPath) {
					retList.add(irodsDirectory.getAbsolutePath()
							+ IRODSFile.PATH_SEPARATOR + l);
				} else {
					retList.add(l);
				}
			}
			return retList;
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		} catch (JargonRuntimeException e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected List<String> search(Map<String, String> metadata)
			throws ReplicationServiceException {
		try {
			DataObjectAO cao = irodsFileSystem.getIRODSAccessObjectFactory()
					.getDataObjectAO(irodsAccount);
			List<AVUQueryElement> queryElements = new ArrayList<AVUQueryElement>();
			for (Map.Entry<String, String> md : metadata.entrySet()) {
				queryElements.add(AVUQueryElement.instanceForValueQuery(
						AVUQueryPart.ATTRIBUTE, AVUQueryOperatorEnum.EQUAL,
						md.getKey()));
				if(md.getValue()!=null) {
					queryElements.add(AVUQueryElement.instanceForValueQuery(
							AVUQueryPart.VALUE, AVUQueryOperatorEnum.EQUAL,
							md.getValue()));
				}
			}
			List<MetaDataAndDomainData> result = cao.findMetadataValuesByMetadataQuery(queryElements);
			List<String> retList = new ArrayList<String>();
			for (MetaDataAndDomainData r : result) {
				retList.add(r.getDomainObjectUniqueName());
			}
			return retList;
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		} catch (JargonQueryException e) {
			throw new ReplicationServiceException(e);
		}
	}

	private SettableJargonProperties overrideJargonProperties = null;

	protected SettableJargonProperties getSettableJargonProperties() {
		IRODSSession irodsSession = irodsFileSystem.getIrodsSession();
		overrideJargonProperties = new SettableJargonProperties(
				irodsSession.getJargonProperties());
		return overrideJargonProperties;
	}
	
	private void overrideJargonProperties(SettableJargonProperties properties) {
    	String maxThreads = configuration.getProperty(B2SAFE_CONFIGURATION.IRODS_TRANSFER_MAX_THREADS.name()).trim();
        
        if(maxThreads!=null) {
        	try{
        		properties.setMaxParallelThreads(Integer.parseInt(maxThreads));
        	}catch(Exception ex) {        		
        	}
        }
        
    }

	/**
	 * @return the information about the IRODS server
	 */
	protected Map<String, String> getServerInformation() throws ReplicationServiceException {
		Map<String, String> info = new HashMap<String, String>();

		try {
			IRODSServerProperties serverProperties = irodsFileSystem
					.getIrodsSession()
					.getDiscoveredServerPropertiesCache()
					.retrieveIRODSServerProperties(irodsAccount.getHost(),
							irodsAccount.getZone());
			if (serverProperties != null){
				info.put("JARGON_VERSION", IRODSServerProperties.getJargonVersion());    	
				info.put("API_VERSION", serverProperties.getApiVersion());
				info.put("REL_VERSION", serverProperties.getRelVersion());
				info.put("RODS_ZONE", serverProperties.getRodsZone());
				info.put("INITIALIZE_DATE", serverProperties.getInitializeDate().toString());
				info.put("SERVER_BOOT_TIME", "" + serverProperties.getServerBootTime());
				info.put("ICAT_ENABLED", serverProperties.getIcatEnabled().toString());    		
			}
			return info;
		} catch (JargonRuntimeException e) {
			throw new ReplicationServiceException(e);
		} catch (NullPointerException e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected void close() throws ReplicationServiceException {
		try {
			irodsFileSystem.close();
		} catch (Exception e) {
			throw new ReplicationServiceException(e);
		}
	}

	protected void finalize() throws Throwable {
		super.finalize();
		this.close();
	}
}
