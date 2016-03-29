package com.mgarciaroig.pfc.fca.export.action;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Class modeling a generic error exporting data from hadoop cluster to database
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ExportException extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ExportException(final String message, final Throwable cause) {
		super(message, cause);		
	}

}
