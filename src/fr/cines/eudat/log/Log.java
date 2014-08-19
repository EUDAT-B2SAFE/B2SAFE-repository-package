package fr.cines.eudat.log;


import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;


/**
 * Outil de journalisation
 * @author Benjamin Watine - CINES
 *
 */
public class Log {
	
	String _log4jXmlPath;
	
	//pour activer le logger faire new Log();
	//changer le chemin  vers le fichier de log
	public Log(){
            
		_log4jXmlPath= "log4j.xml";
                DOMConfigurator.configure(Log.class.getResource(_log4jXmlPath));
          
              
	}
	
	/**
	 * Renvoit le logger utilisé pour les phases d'ingestion
         * @param log
	 * @return Logger
	 */

	public static Logger getLogger(String log) {
		return Logger.getLogger(log);
	}

}
