package com.mgarciaroig.fca.etl.action.discretization.codifier;

import java.io.IOException;

import com.mgarciaroig.pfc.fca.etl.action.discretization.DiscretizationClassifier;
import com.mgarciaroig.pfc.fca.etl.action.discretization.DiscretizationType;
import org.apache.hadoop.conf.Configuration;
import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;

/**
 * Class to filter and obtain required codification strategy for each field type
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class CodifierFilter {

	private DiscretizationClassifier classifier;
	
	private DiscretizationType[] typesOfRequiredFields;
	
	public CodifierFilter(final Configuration configuration, final DiscretizationType ... typesOfRequiredFields) throws IOException{
							
		classifier = new DiscretizationClassifier();			

		this.typesOfRequiredFields = typesOfRequiredFields;
	}
		
		
	public boolean isWanted (final String fieldName) {
																		
		final DiscretizationType fieldDiscretization = getFieldDiscretization(fieldName);
		
		for (final DiscretizationType currentWantedDiscretization: typesOfRequiredFields ){
			if (fieldDiscretization.equals(currentWantedDiscretization)) return true;
		}
		
		return false;							
	}
	
	public boolean isDiscretizableAsEnumeration(final String fieldName){
		
		return matchesDiscretization(fieldName, DiscretizationType.ENUMERATION);		
	}
	
	public boolean isDiscretizableAsBoolean(final String fieldName){
		
		return matchesDiscretization(fieldName, DiscretizationType.BOOLEAN);		
	}
	
	public boolean isDiscretizableAsKmeans(final String fieldName){
						
		return matchesDiscretization(fieldName, DiscretizationType.KMEANS);		
	}	
	
	private boolean matchesDiscretization(final String fieldName, final DiscretizationType wantedDiscretization){
		return getFieldDiscretization(fieldName).equals(wantedDiscretization);
	}
	
	private DiscretizationType getFieldDiscretization(final String fieldName){
		return classifier.classify(Field.buildFieldFromStringfiedRepresentation(fieldName));
	}			
}
