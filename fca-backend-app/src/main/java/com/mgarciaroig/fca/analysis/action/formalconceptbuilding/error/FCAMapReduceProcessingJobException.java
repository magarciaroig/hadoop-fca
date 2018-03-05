package com.mgarciaroig.fca.analysis.action.formalconceptbuilding.error;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Exception modeling an error produced during an fca map-reducer job execution
 * @author Miguel �?ngel García Roig (rocho08@gmail.com)
 *
 */
public class FCAMapReduceProcessingJobException extends FCAAnalizerException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FCAMapReduceProcessingJobException() {
		this(null);
	}
	
	public FCAMapReduceProcessingJobException(final Throwable cause) {
		super("Error processing map-reduce FCA job", cause);

	}
}
