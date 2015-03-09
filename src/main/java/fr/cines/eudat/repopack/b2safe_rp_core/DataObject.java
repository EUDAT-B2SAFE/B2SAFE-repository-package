package fr.cines.eudat.repopack.b2safe_rp_core;

import java.util.HashMap;
import java.util.Map;

import fr.cines.eudat.repopack.b2safe_rp_core.DataSet.B2SAFE_CONFIGURATION;

/**
 * This class represents a data object from the repository. 
 * Even if the data object is replicated as a file, additional information (some metadata) describes the object
 * and its replication.
 * Getter and setter methods are available to handle the different properties
 *  
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
     * Get the data object file MD5 checksum on the local system
     * 
     * @param checksum
     * 		Checksum as a string
     */
    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getRor() {
        return ror;
    }

    public void setRor(String ror) {
        this.ror = ror;
    }

    public String getEudatPid() {
        return eudatPid;
    }

    public void setEudatPid(String eudatPid) {
        this.eudatPid = eudatPid;
    }

    public String getLaunchDate() {
        return launchDate;
    }

    public void setLaunchDate(String replicaLaunchDate) {
        this.launchDate = replicaLaunchDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String replicaSuccessDate) {
        this.endDate = replicaSuccessDate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String error) {
        this.status = error;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String errorMsg) {
        this.statusMessage = errorMsg;
    }

    public String getRemoteDirPath() {
        return remoteDirPath;
    }

    public void setRemoteDirPath(String remoteFilePath) {
        this.remoteDirPath = remoteFilePath;
    }
    
    public boolean getRemoteDirPathIsAbsolute() {
        return remoteDirPathIsAbsolute;
    }

    public void setRemoteDirPathIsAbsolute(boolean isAbsolute) {
        this.remoteDirPathIsAbsolute = isAbsolute;
    }
    
    public void setEudatMetadata(Map<String, AVUMetaData> tmpEudatMetadata) {
    	this.eudatMetadata = tmpEudatMetadata;
    }
    public Map<String, AVUMetaData> getEudatMetadata (){
    	return this.eudatMetadata;
    }
    
    public void addOneEudatMetadata (AVUMetaData avu) {
    	this.eudatMetadata.put(avu.getAttribute(), avu);
    }

    public boolean getOperationIsSuccess() {
    	return (this.getStatus()=="SUCCESS" ? true : false);
    }
    
    public void setOperationIsSuccess() {
    	this.setStatus("SUCCESS");
    }
    
    public void setOperationIsFailure(String errorMessage) {
    	this.setStatus("ERROR");
    	this.setStatusMessage(errorMessage);
    }
    
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