package com.mgarciaroig.pfc.fca.etl.action.dataprepare;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

class IAEADataFieldExtractorBuilder {
	
	private HashMap<Field,String> fieldMap = new HashMap<Field,String>();
	
	private IAEAFieldPropertiesLoader filePropertiesLoader = IAEAFieldPropertiesLoader.createLoader();
	
	IAEADataFieldExtractorBuilder() throws IOException{
				
		this.fieldMap = loadFieldMap();			
	}
	
	IAEADataFieldExtractor buildFieldExtractor(){		
		
		return new IAEADataFieldExtractor(fieldMap);
	}
	
	private HashMap<Field,String> loadFieldMap() throws IOException{
		
		final HashMap<Field,String> map = new HashMap<Field,String>();
		
		final Properties mapProperties = filePropertiesLoader.loadFieldProperties();
		
		final Enumeration<Object> propertyNames = mapProperties.keys();
		while (propertyNames.hasMoreElements()){
			
			final String currentPropertyName =  propertyNames.nextElement().toString();
			final String currentStringfiedCellReference = mapProperties.getProperty(currentPropertyName);
			
			final Field field = Field.valueOf(currentPropertyName);
			
			map.put(field, currentStringfiedCellReference);			
		}
		
		return map;		
	}
}
