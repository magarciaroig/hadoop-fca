package com.mgarciaroig.fca.export.action;

/**
 * Class modeling an error exporting generated objects data from hadoop cluster to database
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
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
