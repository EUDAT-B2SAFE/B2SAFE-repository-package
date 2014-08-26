package fr.cines.eudat.repopack.rp_console;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;

import fr.cines.eudat.repopack.rp_core.AVUMetaData;
import fr.cines.eudat.repopack.rp_core.DataObject;
import fr.cines.eudat.repopack.rp_core.DataSet;

class FileBasedInterface {

	
	protected void writeReplicaResultToFile (ArrayList<DataObject> replicaResult) {

		FileWriter fw = null;
		BufferedWriter bw = null;

		// Get file name from the properties
		try {	 
			File file = new File(main.prop.getProperty("replicationResultFile"));
 
			// if file doesn't exist, then create it
			if (!file.exists()) {
				file.createNewFile();
			} 
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			// Write header
			bw.write("fileName;localFilePath;remoteDirPath;ror;eudatPid;replicaLaunchDate;replicaEndDate;adminStatus;");
			bw.newLine();
			// write one line per data object
			for (DataObject dataObject : replicaResult) {
				bw.write(dataObject.toTextFileOutput());
				bw.newLine();
			}
			bw.close();
		} catch (FileNotFoundException ex) {
			java.util.logging.Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			java.util.logging.Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
		}
		finally
		{

			try 
			{
				//Close the stream of file
				if (bw!= null) bw.close();
			} 
			catch (IOException ex) 
			{
				java.util.logging.Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	protected ArrayList<DataObject> initToReplicateDOList(){
		return textFileToListDO(main.prop.getProperty("localIngestFileList"));
	}
	
	protected ArrayList<DataObject> initToDeleteDOList(){
		return textFileToListDO(main.prop.getProperty("localDeleteFileList"));
	}
	
	protected ArrayList<DataObject> initToRetrieveDOList(){
		return textFileToListDO(main.prop.getProperty("localRetrieveFileList"));
	}
	
	private ArrayList<DataObject> textFileToListDO(String textFilePath){ 
		BufferedReader reader=null;
		File toReadTextFile=null;
		boolean headerLine = true; // used to jump over the header line
		ArrayList<DataObject> resultDOList = new ArrayList<DataObject>();

		try {
			//Read Dataobject in file
			toReadTextFile = new File(textFilePath);

			//Verify if file exists. 
			if(toReadTextFile.exists())
			{
				reader = new BufferedReader(new FileReader(toReadTextFile));
			}

			String line;
			DataObject dataObject;
			if(reader!= null) 
			{    
				while ((line=reader.readLine())!=null)
				{
					//We split each lines from text file to  an array of String 
					String tab[]=line.split(";");
					// Jumps over the header line
					if (headerLine) {
						headerLine = false;
					}
					else {
						// If the line is not empty, set data object values from the fields
						if (tab.length>0)
						{
							dataObject = new DataObject();
							dataObject.setFileName(tab[0]);
							if(tab.length>=1 && !tab[1].equals(""))
							{
								dataObject.setLocalFilePath(tab[1]);
							}
							if(tab.length>=2 && !tab[2].equals(""))
							{
								if ( !tab[2].endsWith("/") ) tab[2] += "/";
								dataObject.setRemoteDirPath(tab[2]);
							}
							if(tab.length>3)
							{
								dataObject.setRor(tab[3]);
								//dataObject.addOneEudatMetadata(new AVUMetaData("ROR",tab[3]));
							}
							if(tab.length>4)
							{
								dataObject.setEudatPid(tab[4]);
							}
							resultDOList.add(dataObject);           
						}
					}
				}

				//Deleting text file after reading it.
				// TODO remove comment for full testing and production
				// ingestFile.delete();	
			}
		} catch (FileNotFoundException ex) {
			java.util.logging.Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			java.util.logging.Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
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
				java.util.logging.Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return resultDOList;
	}
}
