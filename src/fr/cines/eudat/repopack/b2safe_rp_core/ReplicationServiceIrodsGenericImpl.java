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

import fr.cines.eudat.repopack.b2safe_rp_core.ReplicationServiceIRODSImpl.CONFIGURATION;


class ReplicationServiceIrodsGenericImpl implements ReplicationService {
	
	private enum CONFIGURATION {
		HOST,
		PORT,
		USER_NAME,
		PASSWORD,
		HOME_DIRECTORY,
		ZONE,
		DEFAULT_STORAGE,
		REPLICA_DIRECTORY
	}
		
	IRODSFileSystem irodsFileSystem = null;
	IRODSAccount irodsAccount = null;
	Properties configuration = null;
	
	@Override
	public boolean initialize(Properties config) throws ReplicationServiceException {

//		SCn : change to use the properties variable passed in parameter
//		configuration = new Properties();
//		
//		/* copy required configuration */
//		for(CONFIGURATION c : CONFIGURATION.values()) {
//			configuration.put(c.name(), config.get(c.name()));
//		}
		
    	String host = config.getProperty(CONFIGURATION.HOST.name());
    	String port = config.getProperty(CONFIGURATION.PORT.name());
    	String username = config.getProperty(CONFIGURATION.USER_NAME.name());
    	String pass = config.getProperty(CONFIGURATION.PASSWORD.name());
    	String homedir = config.getProperty(CONFIGURATION.HOME_DIRECTORY.name());
    	if ( !homedir.endsWith("/") ) homedir += "/";
    	String zone = config.getProperty(CONFIGURATION.ZONE.name());
    	String default_storage = config.getProperty(CONFIGURATION.DEFAULT_STORAGE.name());
    	    	
		try {
			irodsAccount = IRODSAccount.instance(host, Integer.valueOf(port),
					username, pass, homedir, zone, default_storage);

			irodsFileSystem = IRODSFileSystem.instance();

			AuthResponse response = irodsFileSystem
					.getIRODSAccessObjectFactory().authenticateIRODSAccount(
							irodsAccount);

			return response.isSuccessful();
		} catch (NumberFormatException e) {
			// TODO manage logging
			//log.error("Invalid value of port: " + port, e);
			return false;
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}
	
	@Override
	public boolean isInitialized() {
		return irodsFileSystem != null;
	}
	
	@Override
	public void replicate(String localFileName)
			throws ReplicationServiceException {
		replicate(localFileName, null, false);
	}

	@Override
	public void replicate(String localFileName, Map<String, String> metadata)
			throws ReplicationServiceException {
		replicate(localFileName, metadata, false);
	}

	@Override
	public void replicate(String localFileName, Map<String, String> metadata,
			boolean force) throws ReplicationServiceException {
		String defaultRemoteLocation = configuration
				.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
		replicate(localFileName, defaultRemoteLocation, metadata);
	}

	@Override
	public void replicate(String localFileName, String remoteDirectory,
			Map<String, String> metadata) throws ReplicationServiceException {
		replicate(localFileName, remoteDirectory, metadata, false);
	}

	@Override
	public void replicate(String localFileName, String remoteDirectory, Map<String, String> metadata, boolean force) 
			throws ReplicationServiceException {

		try {
			if(overrideJargonProperties!=null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(overrideJargonProperties);
			}

			File localFile = new File(localFileName);    	

			IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);

			IRODSFile targetDirectory = irodsFileFactory.instanceIRODSFile(irodsAccount.getHomeDirectory() + remoteDirectory);

			if(!targetDirectory.exists()) {
				// SCn : changing to mkdirs as this deals with multiple levels collections
				targetDirectory.mkdirs();
			}

			String targetFile = targetDirectory.getCanonicalPath() + IRODSFile.PATH_SEPARATOR + localFile.getName();

			IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(targetFile);

			DataTransferOperations dataTransferOperations = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getDataTransferOperations(irodsAccount);

			TransferControlBlock controlBlock = irodsFileSystem
					.getIrodsSession()
					.buildDefaultTransferControlBlockBasedOnJargonProperties();

			if(force) {
				controlBlock.getTransferOptions().setForceOption(TransferOptions.ForceOption.USE_FORCE);    		
			}

			dataTransferOperations.putOperation(localFile, remoteFile, null, controlBlock);

			if(metadata==null) {
				metadata = new HashMap<String, String>();
			}

			metadata.put("OTHER_original_checksum", LocalFileUtils.md5ByteArrayToString(LocalFileUtils.computeMD5FileCheckSumViaAbsolutePath(localFile.getAbsolutePath())));
			metadata.put("OTHER_original_filesize", String.valueOf(localFile.length()));

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
	
	@Override
	public boolean delete(String path) throws ReplicationServiceException {
		return delete(path, false);
	}

	public boolean delete(String path, boolean force)
			throws ReplicationServiceException {
		try {

			if (overrideJargonProperties != null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(
						overrideJargonProperties);
			}

			IRODSFileFactory irodsFileFactory = irodsFileSystem
					.getIRODSFileFactory(irodsAccount);
			IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(path);
			if (force) {
				return remoteFile.deleteWithForceOption();
			} else {
				return remoteFile.delete();
			}
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}

	// SCN fix typo in the method name
	@Override
	public void retriveFile(String remoteFileName, String localFileName) 
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

	@Override
	public void addMetadataToDataObject(String filePath,
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
   
	@Override
	public void modifyMetadataToDataObject(String filePath,
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

	@Override
	public void addMetadataToCollection(String collectionPath,
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

	@Override
	public void modifyMetadataToCollection(String collectionPath,
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

	// SCn add a method to read the iCat attributes for a data object stored in B2SAFE
	public Map<String, AVUMetaData> getMetadataOfDataObject(String dataObjectAbsolutePath) 
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
	// End SCn
    
	@Override
	public List<String> list() throws ReplicationServiceException {
		return list(false);
	}

	@Override
	public List<String> list(boolean returnAbsPath)
			throws ReplicationServiceException {
		String defaultRemoteLocation = configuration
				.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
		return list(defaultRemoteLocation, returnAbsPath);
	}

	@Override
	public List<String> list(String remoteDirectory, boolean returnAbsPath)
			throws ReplicationServiceException {
		try {

			if (overrideJargonProperties != null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(
						overrideJargonProperties);
			}

			IRODSFileFactory irodsFileFactory = irodsFileSystem
					.getIRODSFileFactory(irodsAccount);
			IRODSFile irodsDirectory = irodsFileFactory
					.instanceIRODSFile(irodsAccount.getHomeDirectory()
							+ remoteDirectory);
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
		}
	}

	@Override
	public List<String> search(Map<String, String> metadata)
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

	public SettableJargonProperties getSettableJargonProperties() {
		IRODSSession irodsSession = irodsFileSystem.getIrodsSession();
		overrideJargonProperties = new SettableJargonProperties(
				irodsSession.getJargonProperties());
		return overrideJargonProperties;
	}

	/**
	 * @return the information about the IRODS server
	 */
	public IRODSServerProperties gerIRODSServerProperties() {
		return irodsFileSystem
				.getIrodsSession()
				.getDiscoveredServerPropertiesCache()
				.retrieveIRODSServerProperties(irodsAccount.getHost(),
						irodsAccount.getZone());
	}

	@Override
	public void close() throws ReplicationServiceException {
		try {
			irodsFileSystem.close();
		} catch (Exception e) {
			throw new ReplicationServiceException(e);
		}
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		this.close();
	}
}
