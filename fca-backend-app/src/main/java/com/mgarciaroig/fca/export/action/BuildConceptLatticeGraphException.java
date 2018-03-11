package com.mgarciaroig.fca.export.action;

import com.mgarciaroig.fca.framework.error.FCAAnalizerException;

/**
 * Class modeling an error building the concept lattice graph over the database formal concepts
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class BuildConceptLatticeGraphException extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BuildConceptLatticeGraphException(final Throwable cause) {
		this("Error building the concept lattice graph over the database formal concepts", cause);
	}
	
	BuildConceptLatticeGraphException(final String message){
		this(message, null);
	}
	
	BuildConceptLatticeGraphException(final String message, final Throwable cause) {
		super(message, cause);
	}	

}
