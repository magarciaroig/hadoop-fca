package com.mgarciaroig.fca.etl.action.dataprepare;

import java.util.HashMap;

public class IAEAExtractedReactorData {
	
	private HashMap<Field,Object> data = new HashMap<Field,Object>();
	
	public void setFieldData(final Field field, final Object value){
		data.put(field, value);
	}
	
	public Object retriveFieldValue(final Field field){
		return data.get(field);
	}
	
	@Override
	public String toString(){
		
		final String startSerializationSeparator = " { ";
		final String fieldToValueSeparator = " : ";
		final String fieldSeparator = " , ";
		final String endSerializationSeparator = " } ";
		
		final StringBuilder str = new StringBuilder(this.getClass().getSimpleName()).append(startSerializationSeparator);
		
		for (final Field currentField : data.keySet()){
			
			final Object currentValue = data.get(currentField);
			
			str.append(currentField.toString());			
			str.append(fieldToValueSeparator);			
			str.append(currentValue);			
			str.append(fieldSeparator);
		}
				
		str.append(endSerializationSeparator);
		
		return str.toString();		
	}

}
