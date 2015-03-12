package fr.cines.eudat.repopack.b2safe_rp_core;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents a data object from the repository. <br>
 * Even if the data object is replicated as a file, additional information (some metadata) describes the object
 * and its replication.<br>
 * Getter and setter methods are available to handle the different properties<br>
 *  <br>
 * NOTE : This class has the potential to be extended with additional metadata 
 * 
 * @author "S. Coutin (CINES)"
 *
 */
public class DataObject {
	private String operation; 
	private String fileName;
	private String localFilePath;
	private String remoteDirPath;
	private boolean remoteDirPathIsAbsolute = false;
	private String checksum;
	private String ror;
	private String eudatPid;
	private String launchDate;
	private String endDate;
	private String status;
	private String statusMessage;
	private Map<String, AVUMetaData> eudatMetadata = new HashMap<String, AVUMetaData>();
	
	/**
	 * Get the value of the operation applied to the data object
	 * @return
	 * 		String describing the operation, could be REPLICATE, RETRIEVE, DELETE
	 */
    public String getOperation() {
        return operation;
    }

    /**
     * Set the value of the operation applied to the data object
     * @param operation
     * 		String describing the operation, could be REPLICATE, RETRIEVE, DELETE
     */
    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * Get the file name of the data object
     * 
     * @return
     * 		file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Set the file name of the data object
     * 
     * @param fileName
     * 		file name
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Get the local file path for the data object.
     * This is the absolute path.
     * 
     * @return
     * 		Local file absolute path
     */
    public String getLocalFilePath() {
        return localFilePath;
    }

    /**
     * Set the local file path for the data object.
     * This is the absolute path.
     * 
     * @param localFilePath
     * 		Local file absolute path
     */
    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    /**
     * Get the data object file MD5 checksum on the local system
     * 
     * @return
     * 		Checksum as a string
     */
    public String getChecksum() {
        return checksum;
    }

    /**
     * Set the data object file MD5 checksum on the local system
     * 
     * @param checksum
     * 		Checksum as a string
     */
    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    /**
     * Get the ROR (Record Of Reference). This is usually the PID of the object in the repository
     * 
     * @return
     * 		ROR (Record Of Reference)
     */
    public String getRor() {
        return ror;
    }

    /**
     * Set the ROR (Record Of Reference). This is usually the PID of the object in the repository
     * 
     * @param ror
     * 		ROR (Record Of Reference)
     */
    public void setRor(String ror) {
        this.ror = ror;
    }

    /**
     * Get the PID assigned by EUDAT B2SAFE to the first replica
     * 
     * @return
     * 		PID
     */
    public String getEudatPid() {
        return eudatPid;
    }

    /**
     * Set the PID assigned by EUDAT B2SAFE
     * 
     * @param eudatPid
     * 		
     */
    public void setEudatPid(String eudatPid) {
        this.eudatPid = eudatPid;
    }

    /**
     * Get the operation launch date and time
     * 
     * @return
     * 		Launch date as a String
     */
    public String getLaunchDate() {
        return launchDate;
    }

    /**
     * Set the operation launch date and time
     * 
     * @param replicaLaunchDate
     * 		
     */
    protected void setLaunchDate(String replicaLaunchDate) {
        this.launchDate = replicaLaunchDate;
    }

    /**
     * Get the operation completion date and time
     * 
     * @return
     * 		Completion date as a String
     */
    public String getEndDate() {
        return endDate;
    }

    /**
     * Set the operation completion date and time
     * 
     * @param endDate
     * 		
     */
    protected void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    /**
     * Get the operation status
     * 
     * @return
     * 		operation status ("SUCCESS" or "ERROR")
     */
    public String getStatus() {
        return status;
    }

    /**
     * Set the operation status
     * 
     * @param status
     * 		
     */
    protected void setStatus(String status) {
        this.status = status;
    }

    /**
     * Get the message associated to the operation status
     * 
     * @return
     * 		satus message
     */
    public String getStatusMessage() {
        return statusMessage;
    }

    /**
     * Set the message associated to the operation status
     * 
     * @param statusMsg
     * 		
     */
    protected void setStatusMessage(String statusMsg) {
        this.statusMessage = statusMsg;
    }

    /**
     * Get the path to EUDAT B2SAFE directory (named also collection).<br>
     * The path can be either absolute or relative<br>
     * @see setRemoteDirPathIsAbsolute
     * 
     * @return
     * 		EUDAT B2SAFE directory
     */
    public String getRemoteDirPath() {
        return remoteDirPath;
    }

    /**
     * Set the path to EUDAT B2SAFE directory (named also collection).<br>
     * The path can be either absolute or relative.<br>
     * Must be used in conjunction with setRemoteDirPathIsAbsolute method<br>
     * @see setRemoteDirPathIsAbsolute
     * 
     * @param remoteFilePath
     * 		
     */
    public void setRemoteDirPath(String remoteFilePath) {
        this.remoteDirPath = remoteFilePath;
    }
    
    /**
     * Identify whether the EUDAT B2SAFE path is absolute <br>
     * or relative (the basis being the HOME_DIRECTORY property)
     * 
     * @return
     * 		true if path is absolute<br>
     * 		false if path is related (this si the default value)
     */
    public boolean getRemoteDirPathIsAbsolute() {
        return remoteDirPathIsAbsolute;
    }

    /**
     * Define whether the EUDAT B2SAFE path is absolute <br>
     * or relative (the basis being the HOME_DIRECTORY property) 
     * 
     * @param isAbsolute
     * 		true if path is absolute<br>
     * 		false if path is related (this si the default value)
     * 		
     */
    public void setRemoteDirPathIsAbsolute(boolean isAbsolute) {
        this.remoteDirPathIsAbsolute = isAbsolute;
    }
    
    /**
     * Replace all the metadata for the data object
     * 
     * @param tmpEudatMetadata
     * 		
     */
    public void setEudatMetadata(Map<String, AVUMetaData> tmpEudatMetadata) {
    	this.eudatMetadata = tmpEudatMetadata;
    }

    /**
     * Read all the metadata for the data object
     * 
     * @return 
     * 		tmpEudatMetadata map of metadata as Attribute Value Unit
     * 		
     */
    public Map<String, AVUMetaData> getEudatMetadata (){
    	return this.eudatMetadata;
    }
    
    /**
     * Add one metadata to the data object
     * 
     * @param avu
     * 		metadata as Attribute Value Unit
     */
    public void addOneEudatMetadata (AVUMetaData avu) {
    	this.eudatMetadata.put(avu.getAttribute(), avu);
    }

    /**
     * Get the operation status as a boolean
     * 
     * @return
     * 		true if operation succeeded<br>
     * 		false if operation failed
     * 		
     */
    public boolean getOperationIsSuccess() {
    	return (this.getStatus()=="SUCCESS" ? true : false);
    }
    
    /**
     * Set the operation status as being a success
     * 
     */
    protected void setOperationIsSuccess() {
    	this.setStatus("SUCCESS");
    }
    
    /**
     * Set the operation status and status message in case of failure
     * 
     * @param errorMessage
     * 		The message associated with the status
     * 		
     */
    protected void setOperationIsFailure(String errorMessage) {
    	this.setStatus("ERROR");
    	this.setStatusMessage(errorMessage);
    }
    
    /**
     * Get the absolute path to EUDAT B2SAFE directory (named also collection).
     * 
     * @return
     * 		EUDAT B2SAFE directory
     */
    public String getAbsoluteRemoteDirPath(String homeDirectory){
    	return (this.getRemoteDirPathIsAbsolute() ? this.getRemoteDirPath() : homeDirectory + this.getRemoteDirPath());
    }

    /**
     * This returns the data object properties on a readable format. Can be used for logging
     * 
     * @return
     * 		The string representing the data object
     */
    
    @Override
    public String toString()
    {
        
        StringBuilder sb= new StringBuilder();
        sb.append("\r\nDATA OBJECT\r\n ");
        sb.append("fileName => ");
        sb.append(fileName);
        sb.append("\r\n localFilePath => ");
        sb.append(localFilePath);
        sb.append("\r\n remoteFilePath => ");
        if(remoteDirPath!=null)
        {
            sb.append(remoteDirPath);
            sb.append(remoteDirPathIsAbsolute ? " (Absolute)" : " (Relative)");
        }
        else
        {
            sb.append("Default remote path");
        }
                
        if(ror!=null)
        {
             sb.append("\r\n ROR => ");
             sb.append(ror);
        }
        
        if(eudatPid!=null)
        {
             sb.append("\r\n eudatPid => ");
             sb.append(eudatPid);
        }
        
        if(checksum!=null)
        {
             sb.append("\r\n checksum => ");
             sb.append(checksum);
        }
        
        if(launchDate!=null)
        {
             sb.append("\r\n Start date => ");
             sb.append(launchDate);
        }
        
        if(endDate!=null)
        {
             sb.append("\r\n End date => ");
             sb.append(endDate);
        }
        
        if(status!=null)
        {
             sb.append("\r\n adminStatus => ");
             sb.append(status);
        }
        if(statusMessage!=null)
        {
             sb.append(" : ");
             sb.append(statusMessage);
        }
        if (eudatMetadata!=null)
        {
            sb.append("\r\nMETADATA");
			for (Map.Entry<String, AVUMetaData> entry1 : eudatMetadata.entrySet()) {
				sb.append("\r\n"+entry1.toString());
			}
        }
        sb.append(" ]");
        return sb.toString();
    }
        
	
}