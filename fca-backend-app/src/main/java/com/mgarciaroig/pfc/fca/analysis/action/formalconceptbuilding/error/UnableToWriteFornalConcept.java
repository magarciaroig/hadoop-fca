package com.mgarciaroig.pfc.fca.analysis.action.formalconceptbuilding.error;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Class to modelize writing formal concepts errors 
 * @author Miguel �?ngel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class UnableToWriteFornalConcept extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnableToWriteFornalConcept(final Throwable cause) {
		super("Unable to write a formal concept", cause);		
	}

}
