package fr.cines.eudat.repopack.b2safe_rp_pilot;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;

import fr.cines.eudat.repopack.b2safe_rp_core.DataObject;
import fr.cines.eudat.repopack.b2safe_rp_core.DataSet;

/**
 * @author "S. Coutin (CINES)"
 *
 */
class FileBasedInterface {

    /**
     * Used to convert the data object to a string fitting with the rp_console output file format
     * 
     * @return
     * 		The string representing the data object
     */
    private String toTextFileOutput(DataObject dataObject) {
        StringBuilder sb= new StringBuilder();
        sb.append(dataObject.getOperation() +";");
        sb.append(dataObject.getStatus() +";");
        sb.append(dataObject.getStatusMessage() +";");
        sb.append(dataObject.getLaunchDate() + ";");
        sb.append(dataObject.getEndDate() + ";");
        sb.append(dataObject.getFileName() + ";");
        sb.append(dataObject.getLocalFilePath() + ";");
        sb.append(dataObject.getRemoteDirPath() + ";");
        sb.append(dataObject.getRor() + ";");
        sb.append(dataObject.getEudatPid() + ";");
        return sb.toString();  	
    }

    
    /**
     * Used to convert the data object to a string fitting with the rp_console output file format
     * 
     * @return
     * 		The string representing the data object
     */
	protected void writeOperationResultToFile (ArrayList<DataObject> replicaResult) {

		FileWriter fw = null;
		BufferedWriter bw = null;
		boolean writeHeader = false;

		FileWriter fwErr = null;
		BufferedWriter bwErr = null;
		boolean writeHeaderErr = false;

		// Get file name from the properties
		try {	 
			File file = new File(B2safeRpPilot.prop.getProperty("replicationResultFile").trim());
			File fileErr = new File(B2safeRpPilot.prop.getProperty("operationErrorResultFile").trim());
 
			// if file doesn't exist, then create it
			if (!file.exists()) {
				file.createNewFile();
				writeHeader = true;
			} 
			// if file error doesn't exist, then create it
			if (!fileErr.exists()) {
				fileErr.createNewFile();
				writeHeaderErr = true;
			} 
			
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			// Write header
			if (writeHeader) {
				bw.write("Operation;Status;StatusMessage;LaunchDate;EndDate;FileName;LocalFilePath;remoteDirPath;ror;eudatPid;");
				bw.newLine();
			}
			fwErr = new FileWriter(fileErr.getAbsoluteFile(), true);
			bwErr = new BufferedWriter(fwErr);
			// Write header
			if (writeHeaderErr) {
				bwErr.write("Operation;Status;StatusMessage;LaunchDate;EndDate;FileName;LocalFilePath;remoteDirPath;ror;eudatPid;");
				bwErr.newLine();
			}
			// write one line per data object and one line in the error file if status is ERROR
			for (DataObject dataObject : replicaResult) {
				bw.write(toTextFileOutput(dataObject));
				bw.newLine();
				if (dataObject.getStatus().equals("ERROR")) {
					bwErr.write(toTextFileOutput(dataObject));
					bwErr.newLine();					
				}
			}
			bw.close();
			bwErr.close();
			
		} catch (FileNotFoundException ex) {
			B2safeRpPilot.log.error("Text file not found");
		} catch (IOException ex) {
			B2safeRpPilot.log.error("Text file IO exception");
		}
		finally
		{
			try 
			{
				//Close the stream of file
				if (bw!= null) bw.close();
				if (bwErr!= null) bw.close();
			} 
			catch (IOException ex) 
			{
				B2safeRpPilot.log.error("Text file IO exception");
			}
		}
	}

	protected ArrayList<DataObject> initToReplicateDOList(){
		return textFileToListDO(B2safeRpPilot.prop.getProperty("localIngestFileList").trim());
	}
	
	protected ArrayList<DataObject> initToDeleteDOList(){
		return textFileToListDO(B2safeRpPilot.prop.getProperty("localDeleteFileList").trim());
	}
	
	protected ArrayList<DataObject> initToRetrieveDOList(){
		return textFileToListDO(B2safeRpPilot.prop.getProperty("localRetrieveFileList").trim());
	}
	
	private ArrayList<DataObject> textFileToListDO(String textFilePath){ 
		BufferedReader reader=null;
		File toReadTextFile=null;
		boolean headerLine = true; // used to jump over the header line
		ArrayList<DataObject> resultDOList = new ArrayList<DataObject>();

		try {
			//Read Dataobject in file
			B2safeRpPilot.log.debug("Text file to read is : "+ textFilePath);
			toReadTextFile = new File(textFilePath);
			reader = new BufferedReader(new FileReader(toReadTextFile));

			String line;
			DataObject dataObject;
			if(reader!= null) 
			{    
				while ((line=reader.readLine())!=null)
				{
					B2safeRpPilot.log.debug("line is ["+ line+"]");
					// Jumps over the header line
					if (headerLine) {
						headerLine = false;
					}
					else {
						// Check if the line is not empty
						if (line.trim().length()>0)
						{
							//We split each lines from text file to  an array of String 
							String tab[]=line.split(";");
							// If the line is not empty, set data object values from the fields
							if (tab.length>0)
							{
								dataObject = new DataObject();
								dataObject.setFileName(tab[0]);
								if(tab.length>=1 && !tab[1].equals("")) {
									dataObject.setLocalFilePath(tab[1]);
								}
								if(tab.length>=2 && !tab[2].equals("")) {
									if ( !tab[2].endsWith("/") ) tab[2] += "/";
									dataObject.setRemoteDirPath(tab[2]);
								}
								if(tab.length>3) {
									dataObject.setRor(tab[3]);
									//dataObject.addOneEudatMetadata(new AVUMetaData("ROR",tab[3]));
								}
								else {
									dataObject.setRor("None");
								}
								if(tab.length>4) {
									dataObject.setEudatPid(tab[4]);
								}
								resultDOList.add(dataObject);           
							}
						}
					}
				}
			}
		} catch (FileNotFoundException ex) {
			B2safeRpPilot.log.error("Text file not found : "+ textFilePath);
		} catch (IOException ex) {
			B2safeRpPilot.log.error("Text file IO exception : "+ textFilePath);
		}
		finally
		{
			try 
			{
				//Close the stream of file
				if (reader!= null) reader.close();
			} 
			catch (IOException ex) 
			{
				B2safeRpPilot.log.error("Text file IO exception : "+ textFilePath);
			}
		}
		return resultDOList;
	}
}
