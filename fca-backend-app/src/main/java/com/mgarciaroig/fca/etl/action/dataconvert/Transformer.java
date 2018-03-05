package com.mgarciaroig.fca.etl.action.dataconvert;


public interface Transformer {
	
	public String transform(final FieldType fieldType, final String toBeTransformed);
}
