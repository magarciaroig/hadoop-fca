package com.mgarciaroig.pfc.fca.etl.action.dataprepare;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class IAEAFieldPropertiesLoader {
	
	private static IAEAFieldPropertiesLoader loader = null;
	
	public static IAEAFieldPropertiesLoader createLoader(){
		
		if (loader == null){
			loader = new IAEAFieldPropertiesLoader();
		}
		
		return loader;
	}
	
	private IAEAFieldPropertiesLoader(){}		
		
	Properties loadFieldProperties() throws IOException{
		
		final String mappingPropertiesFileName = "field_mapping.properties";
		
		final OrderedProperties mapProperties = new OrderedProperties();		
		
		InputStream stream = null;
		
		try {
		
			stream = this.getClass().getResourceAsStream(mappingPropertiesFileName);
		
			mapProperties.load(stream);
		}
		finally {
			if (stream != null) stream.close();
		}
		
		return mapProperties;
	}

}
