package com.mgarciaroig.fca.framework.error;

/**
 * Base class for exceptions
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FCAAnalizerException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constructor by message
	 * @param message
	 */
	public FCAAnalizerException(String message) {
		super(message);		
	}
	
	/**
	 * Constructor by message and cause
	 * @param message
	 * @param cause
	 */
	public FCAAnalizerException(String message, Throwable cause) {
		super(message, cause);		
	}	
	
	/**
	 * Constructor by cause
	 * @param cause
	 */
	public FCAAnalizerException(Throwable cause) {
		super(cause);
	}

}
