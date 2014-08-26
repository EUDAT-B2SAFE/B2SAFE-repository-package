package fr.cines.eudat.repopack.rp_core;

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


class ReplicationService {
	
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
		
	static IRODSFileSystem irodsFileSystem = null;
	static IRODSAccount irodsAccount = null;
	static Properties configuration = null;
	
	protected static AuthResponse initialize(Properties configuration) throws JargonException {

//		SCn : change to use the properties variable passed in parameter
//		configuration = new Properties();
//		
//		/* copy required configuration */
//		for(CONFIGURATION c : CONFIGURATION.values()) {
//			configuration.put(c.name(), config.get(c.name()));
//		}
		
    	String host = configuration.getProperty(CONFIGURATION.HOST.name());
    	String port = configuration.getProperty(CONFIGURATION.PORT.name());
    	String username = configuration.getProperty(CONFIGURATION.USER_NAME.name());
    	String pass = configuration.getProperty(CONFIGURATION.PASSWORD.name());
    	String homedir = configuration.getProperty(CONFIGURATION.HOME_DIRECTORY.name());
    	if ( !homedir.endsWith("/") ) homedir += "/";
    	String zone = configuration.getProperty(CONFIGURATION.ZONE.name());
    	String default_storage = configuration.getProperty(CONFIGURATION.DEFAULT_STORAGE.name());
    	    	
    	irodsAccount = IRODSAccount.instance(host,
    							Integer.valueOf(port),
    							username,
    							pass,
    							homedir,
    							zone,
    							default_storage);
    	
    	
    	irodsFileSystem = IRODSFileSystem.instance();
    	    	
    	AuthResponse response = irodsFileSystem.getIRODSAccessObjectFactory().authenticateIRODSAccount(irodsAccount);

    	return response;    	
	}
	
	protected static boolean isInitialized() {
		return irodsFileSystem != null;
	}
	
	protected void replicate(String localFileName, Map<String, String> metadata) throws JargonException, IOException {
		replicate(localFileName, metadata, false);	
	}

	protected void replicate(String localFileName, Map<String, String> metadata, boolean force) throws JargonException, IOException {
		String defaultRemoteLocation = configuration.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
		replicate(localFileName, defaultRemoteLocation, metadata);	
	}
	
	protected void replicate(String localFileName, String remoteDirectory, Map<String, String> metadata) throws JargonException, IOException {
		replicate(localFileName, remoteDirectory, metadata, false);
	}
	
	protected void replicate(String localFileName, String remoteDirectory, Map<String, String> metadata, boolean force) throws JargonException, IOException {
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
	}
	
	protected boolean delete(String path) throws JargonException {
		return delete(path, false);
	}

	protected boolean delete(String path, boolean force) throws JargonException {
		IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);
		IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(path);
		if(force) {
			return remoteFile.deleteWithForceOption();
		} else {
			return remoteFile.delete();
		}
	}
	
	// SCN fix typo in the method name
	protected void retrieveFile(String remoteFileName, String localFileName) throws JargonException {
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
	}

	protected void addMetadataToDataObject(String filePath, Map<String, String> metadata) throws JargonException {
    	DataObjectAO dataObjectAO = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		dataObjectAO.addAVUMetadata(filePath, data);
    	}
    }
    
	protected void modifyMetadataToDataObject(String filePath, Map<String, String> metadata) throws JargonException {
    	DataObjectAO dataObjectAO = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		dataObjectAO.modifyAvuValueBasedOnGivenAttributeAndUnit(filePath, data);
    	}
    }
    
	protected void addMetadataToCollection(String collectionPath, Map<String, String> metadata) throws JargonException {
    	CollectionAO collectionAO = irodsFileSystem.getIRODSAccessObjectFactory().getCollectionAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		collectionAO.addAVUMetadata(collectionPath, data);
    	}
    }
    
	protected void modifyMetadataToCollection(String collectionPath, Map<String, String> metadata) throws JargonException {
    	CollectionAO collectionAO = irodsFileSystem.getIRODSAccessObjectFactory().getCollectionAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		collectionAO.modifyAvuValueBasedOnGivenAttributeAndUnit(collectionPath, data);
    	}
    }
    
    // SCn add a method to read the iCat attributes for a data object stored in B2SAFE
	protected Map<String, AVUMetaData> getMetadataOfDataObject(String dataObjectAbsolutePath) throws JargonException {
    	Map<String, AVUMetaData> eudatMetadata = new HashMap<String, AVUMetaData>();
    	DataObjectAO dataObjectAO = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);
    	
    	List<MetaDataAndDomainData> listMetaData = dataObjectAO.findMetadataValuesForDataObject(dataObjectAbsolutePath);
    	for (MetaDataAndDomainData temp : listMetaData) {
    		// System.out.println(temp.toString());
    		eudatMetadata.put(temp.getAvuAttribute(), new AVUMetaData(temp.getAvuAttribute(),temp.getAvuValue(),temp.getAvuUnit()));
    	}
     
    	return eudatMetadata;
    	
    }
    // End SCn
    
	protected List<String> list() throws JargonException {
    	return list(false);    	
    }
    
	protected List<String> list(boolean returnAbsPath) throws JargonException {
    	String defaultRemoteLocation = configuration.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
    	return list(defaultRemoteLocation, returnAbsPath);
    }
    
	protected List<String> list(String remoteDirectory, boolean returnAbsPath) throws JargonException {
    	IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);    	
    	IRODSFile irodsDirectory = irodsFileFactory.instanceIRODSFile(irodsAccount.getHomeDirectory() + remoteDirectory);    	
    	String[] list = irodsDirectory.list();
    	List<String> retList = new ArrayList<String>();
    	for(String l : list) {
    		if(returnAbsPath) {
    			retList.add(irodsDirectory.getAbsolutePath() + IRODSFile.PATH_SEPARATOR + l);
    		} else {
    			retList.add(l);
    		}
    	}
    	return retList;
    }

	protected List<String> search(Map<String, String> metadata) throws JargonException, JargonQueryException {
    	DataObjectAO cao = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);
    	List<AVUQueryElement> queryElements = new ArrayList<AVUQueryElement>();
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		queryElements.add(AVUQueryElement.instanceForValueQuery(AVUQueryPart.ATTRIBUTE, AVUQueryOperatorEnum.EQUAL, md.getKey()));
    		queryElements.add(AVUQueryElement.instanceForValueQuery(AVUQueryPart.VALUE, AVUQueryOperatorEnum.EQUAL, md.getValue()));
    	}
    	List<MetaDataAndDomainData> result = cao.findMetadataValuesByMetadataQuery(queryElements);
    	List<String> retList = new ArrayList<String>();
    	for(MetaDataAndDomainData r : result) {
    		retList.add(r.getDomainObjectUniqueName());
    	}
    	return retList;
    }
        
	protected static SettableJargonProperties overrideJargonProperties = null;
    
	protected static SettableJargonProperties getSettableJargonProperties() {
    	IRODSSession irodsSession = irodsFileSystem.getIrodsSession();
    	overrideJargonProperties = new SettableJargonProperties(irodsSession.getJargonProperties());
		return overrideJargonProperties;
    }
    
	protected static IRODSServerProperties gerIRODSServerProperties() {
    	return irodsFileSystem.getIrodsSession()
    				.getDiscoveredServerPropertiesCache()
    				.retrieveIRODSServerProperties(irodsAccount.getHost(), irodsAccount.getZone());
    }
    
    @Override
    protected void finalize() throws Throwable {
    	super.finalize();
    	irodsFileSystem.close();
    }
    
}
