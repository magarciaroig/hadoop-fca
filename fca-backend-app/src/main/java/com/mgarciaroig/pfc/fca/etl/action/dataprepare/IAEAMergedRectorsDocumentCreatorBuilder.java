package com.mgarciaroig.pfc.fca.etl.action.dataprepare;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

class IAEAMergedRectorsDocumentCreatorBuilder {
	
	final List<Field> fieldsInOrder;
			
	IAEAMergedRectorsDocumentCreatorBuilder() throws IOException{
				
		this.fieldsInOrder = retrieveFieldsInOrder();
	}
	
	IAEAMergedRectorsDocumentCreator buildDocumentCreator() throws IOException{
						
		return new IAEAMergedRectorsDocumentCreator(fieldsInOrder);		
	}
	
	private List<Field> retrieveFieldsInOrder() throws IOException{
		
		final IAEAFieldPropertiesLoader filePropertiesLoader = IAEAFieldPropertiesLoader.createLoader();
		
		final List<Field> fieldsInOrder = new  ArrayList<Field>();
		
		final Properties fieldsProperties = filePropertiesLoader.loadFieldProperties();
		
		final Enumeration<Object> fieldsEnumeration = fieldsProperties.keys();
		
		while (fieldsEnumeration.hasMoreElements()){
			
			final String currentFieldName = fieldsEnumeration.nextElement().toString();
			fieldsInOrder.add(Field.valueOf(currentFieldName));
		}				
		
		return fieldsInOrder;
	}

}
