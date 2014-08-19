package fr.cines.eudat.repopack.rp_core;

import java.util.HashMap;
import java.util.Map;

public class DataObject {
	private String repositoryIdentifier;
	private String fileName;
	private String localFilePath;
	private String remoteDirPath;
	private String checksum;
	private String ror;
	private String eudatPid;
	private String replicaLaunchDate;
	private String replicaEndDate;
	private String adminStatus;
	private Map<String, AVUMetaData> eudatMetadata = new HashMap<String, AVUMetaData>();
	
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

    public String getReplicaLaunchDate() {
        return replicaLaunchDate;
    }

    public void setReplicaLaunchDate(String replicaLaunchDate) {
        this.replicaLaunchDate = replicaLaunchDate;
    }

    public String getReplicaSuccessDate() {
        return replicaEndDate;
    }

    public void setReplicaSuccessDate(String replicaSuccessDate) {
        this.replicaEndDate = replicaSuccessDate;
    }

    public String setAdminStatus() {
        return adminStatus;
    }

    public void setAdminStatus(String error) {
        this.adminStatus = error;
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
        
    public String toTextFileOutput() {
        StringBuilder sb= new StringBuilder();
        sb.append(this.fileName + ";");
        sb.append(this.localFilePath + ";");
        sb.append(this.remoteDirPath + ";");
        sb.append(this.ror + ";");
        sb.append(this.eudatPid + ";");
        sb.append(this.replicaLaunchDate + ";");
        sb.append(this.replicaEndDate + ";");
        sb.append(this.adminStatus +";");
        return sb.toString();  	
    }
    
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
        
        if(replicaLaunchDate!=null)
        {
             sb.append(", Start date => ");
             sb.append(replicaLaunchDate);
        }
        
        if(replicaEndDate!=null)
        {
             sb.append(", End date => ");
             sb.append(replicaEndDate);
        }
        
        if(adminStatus!=null)
        {
             sb.append(", adminStatus => ");
             sb.append(adminStatus);
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