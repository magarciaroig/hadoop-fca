package com.mgarciaroig.fca.etl.action.dataimport;

import com.mgarciaroig.pfc.fca.framework.error.ReactorsFCAAnalizerException;

/**
 * Class modelizing an error importing IAEA data file from remote location 
 * 
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class BadIAEADateImportUrlException extends ReactorsFCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final String  IAEAinputDataUrl;

	/**
	 * Class constructor
	 * @param IAEAinputDataUrl
	 * @param cause 
	 */
	public BadIAEADateImportUrlException(final String IAEAinputDataUrl, Throwable cause) {
		
		super(buildErrorMessage(IAEAinputDataUrl), cause);
		
		this.IAEAinputDataUrl = IAEAinputDataUrl;
	}

	private static String buildErrorMessage(final String IAEAinputDataUrl) {
		return String.format("Bad IAEA data import url '%s'", IAEAinputDataUrl);
	}

	/**
	 * IAEA input data url
	 * @return
	 */
	public String getIAEAinputDataUrl() {
		return IAEAinputDataUrl;
	}		
}
