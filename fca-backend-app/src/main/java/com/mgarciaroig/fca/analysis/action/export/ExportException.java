package com.mgarciaroig.fca.analysis.action.export;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Class modeling a generic error exporting data from hadoop cluster to database
 * @author Miguel �?ngel García Roig (rocho08@gmail.com)
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
