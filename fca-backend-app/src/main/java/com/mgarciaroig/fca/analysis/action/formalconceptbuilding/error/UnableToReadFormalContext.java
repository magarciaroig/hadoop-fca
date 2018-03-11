package com.mgarciaroig.fca.analysis.action.formalconceptbuilding.error;

import com.mgarciaroig.fca.framework.error.FCAAnalizerException;

/**
 * Class to modelize reading formal context items errors 
 * @author Miguel �?ngel García Roig (rocho08@gmail.com)
 *
 */
public class UnableToReadFormalContext extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnableToReadFormalContext(final Throwable cause) {
		super("Unable to read a formal context item", cause);
	}
}
