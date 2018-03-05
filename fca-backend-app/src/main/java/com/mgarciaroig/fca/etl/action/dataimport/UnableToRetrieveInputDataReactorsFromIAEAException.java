package com.mgarciaroig.fca.etl.action.dataimport;

import java.net.URL;

import org.apache.hadoop.fs.Path;

import com.mgarciaroig.pfc.fca.framework.error.ReactorsFCAAnalizerException;

/**
 * Modelizes an error importing remote reactors file 
 * 
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class UnableToRetrieveInputDataReactorsFromIAEAException extends
		ReactorsFCAAnalizerException {
	
	private final URL inputDataUrl; 
	
	private final Path outputPath;
	
	private final String outputFileName;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
		
	/**
	 * Class constructor
	 * @param inputDataUrl Remote file
	 * @param outputPath Destination folder in the cluster
	 * @param outputFileName Destination file name in the cluster
	 * @param cause
	 */
	public UnableToRetrieveInputDataReactorsFromIAEAException(final URL inputDataUrl, final Path outputPath, final String outputFileName, Throwable cause) {
		super(buildErrorMessage(inputDataUrl, outputPath, outputFileName), cause);
		
		this.inputDataUrl = inputDataUrl;
		this.outputPath = outputPath;
		this.outputFileName = outputFileName;		
	}

	private static String buildErrorMessage(final URL inputDataUrl,
			final Path outputPath, final String outputFileName) {
		return String.format("Unable to import reactors data from %s to %s/%s", inputDataUrl.toString(), outputPath.toString(), outputFileName);
	}

	/**
	 * Remote file
	 * @return
	 */
	public URL getInputDataUrl() {
		return inputDataUrl;
	}

	/**
	 * Destination folder in the cluster
	 * @return
	 */
	public Path getOutputPath() {
		return outputPath;
	}

	/**
	 * Destination file name in the cluster
	 * @return
	 */
	public String getOutputFileName() {
		return outputFileName;
	}		
}
