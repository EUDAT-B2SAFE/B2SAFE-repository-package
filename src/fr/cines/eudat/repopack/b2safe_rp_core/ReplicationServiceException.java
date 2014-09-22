/**
 * Institute of Formal and Applied Linguistics
 * Charles University in Prague, Czech Republic
 * 
 * http://ufal.mff.cuni.cz
 * 
 */

package fr.cines.eudat.repopack.b2safe_rp_core;

/**
 * Wrapper for all exceptions in ReplicationService implementation
 * 
 * @author Amir Kamran
 * 
 */

public class ReplicationServiceException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Instantiate a new ReplicationServiceException
	 * 
	 */
	public ReplicationServiceException() {
	}

	/**
	 * Instantiate a new ReplicationServiceException 
	 * 
	 * @param t the root cause
	 */
	public ReplicationServiceException(Throwable t) {
		super(t);
	}

	/**
	 * Instantiate a new ReplicationServiceException
	 * 
	 * @param message the error message
	 */
	public ReplicationServiceException(String message) {
		super(message);
	}

}
