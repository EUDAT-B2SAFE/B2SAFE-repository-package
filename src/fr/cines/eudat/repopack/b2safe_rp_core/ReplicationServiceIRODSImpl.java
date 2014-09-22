/**
 * Institute of Formal and Applied Linguistics
 * Charles University in Prague, Czech Republic
 * 
 * http://ufal.mff.cuni.cz
 * 
 */

package fr.cines.eudat.repopack.b2safe_rp_core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
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

/**
 * Implementation of ReplicationService based on IRODS
 * 
 * @author Amir Kamran
 * 
 */

public class ReplicationServiceIRODSImpl implements ReplicationService {

	Logger log = Logger.getLogger(ReplicationServiceIRODSImpl.class);

	/**
	 * enum for the required configuration for IRODS connection
	 * 
	 */
	public enum CONFIGURATION {
		HOST, PORT, USER_NAME, PASSWORD, HOME_DIRECTORY, ZONE, DEFAULT_STORAGE, REPLICA_DIRECTORY
	}

	IRODSFileSystem irodsFileSystem = null;

	IRODSAccount irodsAccount = null;

	Properties configuration = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#initialize(java.util.Properties
	 * )
	 */
	@Override
	public boolean initialize(Properties config)
			throws ReplicationServiceException {

		configuration = new Properties();

		/* copy required configuration */
		for (CONFIGURATION c : CONFIGURATION.values()) {
			configuration.put(c.name(), config.getProperty(c.name(), ""));
		}

		String host = configuration.getProperty(CONFIGURATION.HOST.name());
		String port = configuration.getProperty(CONFIGURATION.PORT.name());
		String username = configuration.getProperty(CONFIGURATION.USER_NAME
				.name());
		String pass = configuration.getProperty(CONFIGURATION.PASSWORD.name());
		String homedir = configuration.getProperty(CONFIGURATION.HOME_DIRECTORY
				.name());
		if (!homedir.endsWith("/"))
			homedir += "/";
		String zone = configuration.getProperty(CONFIGURATION.ZONE.name());
		String default_storage = configuration
				.getProperty(CONFIGURATION.DEFAULT_STORAGE.name());

		try {
			irodsAccount = IRODSAccount.instance(host, Integer.valueOf(port),
					username, pass, homedir, zone, default_storage);

			irodsFileSystem = IRODSFileSystem.instance();

			AuthResponse response = irodsFileSystem
					.getIRODSAccessObjectFactory().authenticateIRODSAccount(
							irodsAccount);

			return response.isSuccessful();
		} catch (NumberFormatException e) {
			log.error("Invalid value of port: " + port, e);
			return false;
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#isInitialized()
	 */
	@Override
	public boolean isInitialized() {
		return irodsFileSystem != null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#replicate(java.lang.String)
	 */
	@Override
	public void replicate(String localFileName)
			throws ReplicationServiceException {
		replicate(localFileName, null, false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#replicate(java.lang.String,
	 * java.util.Map)
	 */
	@Override
	public void replicate(String localFileName, Map<String, String> metadata)
			throws ReplicationServiceException {
		replicate(localFileName, metadata, false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#replicate(java.lang.String,
	 * java.util.Map, boolean)
	 */
	@Override
	public void replicate(String localFileName, Map<String, String> metadata,
			boolean force) throws ReplicationServiceException {
		String defaultRemoteLocation = configuration
				.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
		replicate(localFileName, defaultRemoteLocation, metadata);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#replicate(java.lang.String,
	 * java.lang.String, java.util.Map)
	 */
	@Override
	public void replicate(String localFileName, String remoteDirectory,
			Map<String, String> metadata) throws ReplicationServiceException {
		replicate(localFileName, remoteDirectory, metadata, false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#replicate(java.lang.String,
	 * java.lang.String, java.util.Map, boolean)
	 */
	@Override
	public void replicate(String localFileName, String remoteDirectory,
			Map<String, String> metadata, boolean force)
			throws ReplicationServiceException {
		try {
			if (overrideJargonProperties != null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(
						overrideJargonProperties);
			}

			File localFile = new File(localFileName);

			IRODSFileFactory irodsFileFactory = irodsFileSystem
					.getIRODSFileFactory(irodsAccount);

			IRODSFile targetDirectory = irodsFileFactory
					.instanceIRODSFile(irodsAccount.getHomeDirectory()
							+ remoteDirectory);

			if (!targetDirectory.exists()) {
				targetDirectory.mkdirs();
			}

			String targetFile = targetDirectory.getCanonicalPath()
					+ IRODSFile.PATH_SEPARATOR + localFile.getName();

			IRODSFile remoteFile = irodsFileFactory
					.instanceIRODSFile(targetFile);

			DataTransferOperations dataTransferOperations = irodsFileSystem
					.getIRODSAccessObjectFactory().getDataTransferOperations(
							irodsAccount);

			TransferControlBlock controlBlock = irodsFileSystem
					.getIrodsSession()
					.buildDefaultTransferControlBlockBasedOnJargonProperties();

			if (force) {
				controlBlock.getTransferOptions().setForceOption(
						TransferOptions.ForceOption.USE_FORCE);
			}

			dataTransferOperations.putOperation(localFile, remoteFile, null,
					controlBlock);

			if (metadata == null) {
				metadata = new HashMap<String, String>();
			}

			metadata.put("OTHER_original_checksum", Hex
					.encodeHexString(LocalFileUtils
							.computeMD5FileCheckSumViaAbsolutePath(localFile
									.getAbsolutePath())));
			metadata.put("OTHER_original_filesize",
					String.valueOf(localFile.length()));

			if (localFile.isDirectory()) {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#delete(java.lang.String)
	 */
	@Override
	public boolean delete(String path) throws ReplicationServiceException {
		return delete(path, false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#delete(java.lang.String,
	 * boolean)
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#retriveFile(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void retriveFile(String remoteFileName, String localFileName)
			throws ReplicationServiceException {
		try {
			if (overrideJargonProperties != null) {
				irodsFileSystem.getIrodsSession().setJargonProperties(
						overrideJargonProperties);
			}
			File localFile = new File(localFileName);

			IRODSFileFactory irodsFileFactory = irodsFileSystem
					.getIRODSFileFactory(irodsAccount);

			IRODSFile remoteFile = irodsFileFactory
					.instanceIRODSFile(remoteFileName);

			DataTransferOperations dataTransferOperations = irodsFileSystem
					.getIRODSAccessObjectFactory().getDataTransferOperations(
							irodsAccount);

			dataTransferOperations.getOperation(remoteFile.getAbsolutePath(),
					localFile.getAbsolutePath(), "", null, null);
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#addMetadataToDataObject(java
	 * .lang.String, java.util.Map)
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#modifyMetadataToDataObject(
	 * java.lang.String, java.util.Map)
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#addMetadataToCollection(java
	 * .lang.String, java.util.Map)
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#modifyMetadataToCollection(
	 * java.lang.String, java.util.Map)
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cz.cuni.mff.ufal.b2safe.ReplicationSerice#getMetadataOfDataObject(java
	 * .lang.String)
	 */
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
    
/*	@Override
	public Map<String, String> getMetadataOfDataObject(
			String dataObjectAbsolutePath) throws ReplicationServiceException {
		try {
			Map<String, String> metadata = new HashMap<String, String>();
			DataObjectAO dataObjectAO = irodsFileSystem
					.getIRODSAccessObjectFactory()
					.getDataObjectAO(irodsAccount);
			List<MetaDataAndDomainData> listMetaData = dataObjectAO
					.findMetadataValuesForDataObject(dataObjectAbsolutePath);
			for (MetaDataAndDomainData lmd : listMetaData) {
				metadata.put(lmd.getAvuAttribute(), lmd.getAvuValue());
			}
			return metadata;
		} catch (JargonException e) {
			throw new ReplicationServiceException(e);
		}
	}
*/
	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#list()
	 */
	@Override
	public List<String> list() throws ReplicationServiceException {
		return list(false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#list(boolean)
	 */
	@Override
	public List<String> list(boolean returnAbsPath)
			throws ReplicationServiceException {
		String defaultRemoteLocation = configuration
				.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
		return list(defaultRemoteLocation, returnAbsPath);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#list(java.lang.String,
	 * boolean)
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#search(java.util.Map)
	 */
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

	/**
	 * call to this will create a modifiable jargon properties object the
	 * modified properties will be used for all operations
	 * 
	 * @return the modifiable jargon properties object.
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see cz.cuni.mff.ufal.b2safe.ReplicationSerice#close()
	 */
	@Override
	public void close() throws ReplicationServiceException {
		try {
			irodsFileSystem.close();
		} catch (Exception e) {
			throw new ReplicationServiceException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		this.close();
	}

}
