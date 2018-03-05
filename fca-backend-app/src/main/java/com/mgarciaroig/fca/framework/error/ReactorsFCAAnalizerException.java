package com.mgarciaroig.fca.framework.error;

/**
 * Higher application error abstraction
 * 
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ReactorsFCAAnalizerException extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * Constructor by message
	 * @param message
	 */
	public ReactorsFCAAnalizerException(String message) {
		super(message);		
	}
	
	/**
	 * Constructor by message and cause
	 * @param message
	 * @param cause
	 */
	public ReactorsFCAAnalizerException(String message, Throwable cause) {
		super(message, cause);		
	}	
	
	/**
	 * Constructor by cause
	 * @param cause
	 */
	public ReactorsFCAAnalizerException(Throwable cause) {
		super(cause);
	}

}
