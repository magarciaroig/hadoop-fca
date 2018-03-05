package com.mgarciaroig.fca.etl.action.discretization;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;

/**
 * Classifies fields in function of the required codification strategy for its values
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class DiscretizationClassifier {
	
	private final Properties discretizationProperties;
	
	public DiscretizationClassifier() throws IOException{
		discretizationProperties = loadDiscretizationProperties();
	}

	public DiscretizationType classify(final Field toBeClassified){
		
		DiscretizationType discretization = DiscretizationType.NONE; 
		
		if (toBeClassified != null){
		
			final String stringfiedDiscretization = discretizationProperties.getProperty(toBeClassified.toString());
		
			if (stringfiedDiscretization != null){
				discretization = DiscretizationType.buildFromCode(stringfiedDiscretization);
			}
		}
		
		return discretization;
	}		
	
	private Properties loadDiscretizationProperties() throws IOException{
		
		final Properties discretizationProperties = new Properties();
		InputStream stream = null;
		
		try {
			
			stream = getClass().getResourceAsStream("discretization_config.properties");			
			discretizationProperties.load(stream);
			
		}
		finally {
			if (stream != null) stream.close();
		}
		
		return discretizationProperties;
	}
}
