package com.mgarciaroig.fca.export.action;

import com.mgarciaroig.fca.framework.error.FCAAnalizerException;

/**
 * Class modeling a generic error exporting data from hadoop cluster to database
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
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
