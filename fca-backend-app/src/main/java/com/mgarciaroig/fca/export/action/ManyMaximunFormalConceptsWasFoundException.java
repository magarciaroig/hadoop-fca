package com.mgarciaroig.fca.export.action;

/**
 * Class modeling an error when many max formal concept were found in the database
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class ManyMaximunFormalConceptsWasFoundException extends BuildConceptLatticeGraphException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ManyMaximunFormalConceptsWasFoundException() {
		super("Many maximum formal concept were found in the database");
	}
}
