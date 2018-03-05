package com.mgarciaroig.fca.analysis.action.formalconceptbuilding.error;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Error modeling an error produced when, after the max allowed iterations has been reached, all formal concepts has not been computed yet
 * @author Miguel �?ngel García Roig (rocho08@gmail.com)
 *
 */
public class FCAAlgorythmDoesNotConvergeException extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final int maxIterations;

	
	public FCAAlgorythmDoesNotConvergeException(int maxIterations) {
		super(buildErrorMessage(maxIterations));		
		
		this.maxIterations = maxIterations;
	}	

	public int getMaxIterations() {
		return maxIterations;
	}
	
	private static String buildErrorMessage(int maxIterations) {
		return String.format("FCA algorythm does not converge after %d iterations", maxIterations);
	}
}
