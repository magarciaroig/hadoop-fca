package com.mgarciaroig.pfc.fca.analysis.action.formalconceptbuilding.error;

import java.io.IOException;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Exception modeling an I/O error produced during fca processing operations
 * @author Miguel �?ngel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FCAFilesCreationException extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	public FCAFilesCreationException(final IOException cause) {
		super("Error creating files during fca algorythm computation", cause);		
	}

}
