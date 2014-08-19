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

public class FileBasedInterface {

	protected ArrayList<DataObject> initToReplicateDOList(){
		BufferedReader reader=null;
		File ingestFile=null;
		ArrayList<DataObject> toReplicateDOList = new ArrayList<DataObject>();

		// Get file name from the properties
		String toReplicateDataObjectsFilePath = main.prop.getProperty("tmpLocalIngestFileList");
		try {
			//Read Dataobject to replicate in DataObjectToReplicate.txt file
			ingestFile = new File(toReplicateDataObjectsFilePath);

			//Verify if DataObjectToReplicate.txt file exists. 
			if(ingestFile.exists())
			{
				reader = new BufferedReader(new FileReader(ingestFile));
			}

			String line;
			DataObject dataObject;
			if(reader!= null) 
			{    while ((line=reader.readLine())!=null)
			{
				//We split each lines from DataObjectToReplicate.txt file to  an array of String 
				String tab[]=line.split(";");

				if(tab.length>0)
				{
					dataObject = new DataObject();
					dataObject.setFileName(tab[0]);
					dataObject.setLocalFilePath(tab[1]);
					if(tab.length>=2 && !tab[2].equals(""))
					{
						dataObject.setRemoteDirPath(tab[2]);
					}
					if(tab.length>=3 && !tab[3].equals(""))
					{
						dataObject.setRor(tab[3]);
						dataObject.addOneEudatMetadata(new AVUMetaData("ROR",tab[3]));
					}

					toReplicateDOList.add(dataObject);           
					// For dev only
					System.out.println(dataObject.toString());
				}   
			}

			//Deleting DataObjectToReplicate.txt file after reading it.
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
		return toReplicateDOList;
	}
	
	protected void writeReplicaResultToFile (ArrayList<DataObject> replicaResult) {

		FileWriter fw = null;
		BufferedWriter bw = null;

		// Get file name from the properties
		try {	 
			File file = new File(main.prop.getProperty("replicationResultFile"));
 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			for (DataObject dataObject : replicaResult) {
				//bw.write(dataObject.toString());
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

	protected ArrayList<DataObject> initToDeleteDOList(){
		BufferedReader reader=null;
		File toDeleteFile=null;
		ArrayList<DataObject> toDeleteDOList = new ArrayList<DataObject>();

		// Get file name from the properties
		String toDeleteDataObjectsFilePath = main.prop.getProperty("tmpLocalDeleteFileList");
		try {
			//Read Dataobject in file
			toDeleteFile = new File(toDeleteDataObjectsFilePath);

			//Verify if DataObjectToReplicate.txt file exists. 
			if(toDeleteFile.exists())
			{
				reader = new BufferedReader(new FileReader(toDeleteFile));

			}

			String line;
			DataObject dataObject;
			if(reader!= null) 
			{    while ((line=reader.readLine())!=null)
			{
				//We split each lines from DataObjectToReplicate.txt file to  an array of String 
				String tab[]=line.split(";");

				if(tab.length>0)
				{
					dataObject = new DataObject();
					dataObject.setFileName(tab[0]);
					dataObject.setRemoteDirPath(tab[1]);

					toDeleteDOList.add(dataObject);           
					// For dev only
					System.out.println(dataObject.toString());
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
		return toDeleteDOList;
	}

}
