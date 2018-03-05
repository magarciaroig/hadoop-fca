package com.mgarciaroig.fca.export.action;

/**
 * Class modeling an error exporting generated formal concepts data from hadoop cluster to database
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ExportFormalConceptsException extends ExportException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ExportFormalConceptsException(final Throwable cause) {
		super("Error exporting formal concepts data to database", cause);
	}

}
