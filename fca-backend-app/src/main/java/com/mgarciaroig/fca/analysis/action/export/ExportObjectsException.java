package com.mgarciaroig.fca.analysis.action.export;

/**
 * Class modeling an error exporting generated objects data from hadoop cluster to database
 * @author Miguel �?ngel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ExportObjectsException extends ExportException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ExportObjectsException(final Throwable cause) {
		super("Error exporting objects data to database", cause);		
	}

}
