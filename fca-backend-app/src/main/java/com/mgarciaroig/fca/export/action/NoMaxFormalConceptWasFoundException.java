package com.mgarciaroig.fca.export.action;

/**
 * Class modeling an error when no max formal concept is found in the database
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class NoMaxFormalConceptWasFoundException extends BuildConceptLatticeGraphException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoMaxFormalConceptWasFoundException() {
		super("No maximun formal concept was found in the database");		
	}

}
