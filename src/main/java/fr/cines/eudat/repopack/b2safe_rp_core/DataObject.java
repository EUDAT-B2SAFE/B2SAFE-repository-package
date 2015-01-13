package fr.cines.eudat.repopack.b2safe_rp_core;

import java.util.HashMap;
import java.util.Map;

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
	private String repositoryIdentifier;
	private String fileName;
	private String localFilePath;
	private String remoteDirPath;
	private String checksum;
	private String ror;
	private String eudatPid;
	private String launchDate;
	private String endDate;
	private String status;
	private String statusMessage;
	private Map<String, AVUMetaData> eudatMetadata = new HashMap<String, AVUMetaData>();
	
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getRepositoryIdentifier() {
        return repositoryIdentifier;
    }

    public void setRepositoryIdentifier(String repositoryIdentifier) {
        this.repositoryIdentifier = repositoryIdentifier;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    public String getChecksum() {
        return checksum;
    }

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
    
    public void setEudatMetadata(Map<String, AVUMetaData> tmpEudatMetadata) {
    	this.eudatMetadata = tmpEudatMetadata;
    }
    public Map<String, AVUMetaData> getEudatMetadata (){
    	return this.eudatMetadata;
    }
    
    public void addOneEudatMetadata (AVUMetaData avu) {
    	this.eudatMetadata.put(avu.getAttribute(), avu);
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
        sb.append("[fileName => ");
        sb.append(fileName);
        sb.append(", localFilePath => ");
        sb.append(localFilePath);
        sb.append(", remoteFilePath => ");
        if(remoteDirPath!=null)
        {
            sb.append(remoteDirPath);
        }
        else
        {
            //String path=EudatIngestionSiteIrods.homeDirectory+EudatIngestionSiteIrods.scratchDirectory;
            sb.append("Default remote path");
        }
                
        if(ror!=null)
        {
             sb.append(", ROR => ");
             sb.append(ror);
        }
        
        if(eudatPid!=null)
        {
             sb.append(", eudatPid => ");
             sb.append(eudatPid);
        }
        
        if(checksum!=null)
        {
             sb.append(", checksum => ");
             sb.append(checksum);
        }
        
        if(launchDate!=null)
        {
             sb.append(", Start date => ");
             sb.append(launchDate);
        }
        
        if(endDate!=null)
        {
             sb.append(", End date => ");
             sb.append(endDate);
        }
        
        if(status!=null)
        {
             sb.append(", adminStatus => ");
             sb.append(status);
        }
        if (eudatMetadata!=null)
        {
			for (Map.Entry<String, AVUMetaData> entry1 : eudatMetadata.entrySet()) {
				sb.append(entry1.toString());
			}
        }
        sb.append(" ]");
        return sb.toString();
    }
        
	
}