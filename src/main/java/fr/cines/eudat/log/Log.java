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

		// imagine all dependencies would ship their own log4j property file
		// and would require different parameters for log directory...
		// however, we can do two small hacks to make it usable!

		// do not even try to output to "/debug.log"
		if ( null == System.getProperty("RP_LOG_DIR") ) {
			return;
		}

		// do not use log4j.xml which might get loaded by default!!!
		_log4jXmlPath= "b2safe-log4j.xml";
        // DOMConfigurator.configure(Log.class.getResource(_log4jXmlPath));
        DOMConfigurator.configure(this.getClass().getClassLoader().getResource(_log4jXmlPath));
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
