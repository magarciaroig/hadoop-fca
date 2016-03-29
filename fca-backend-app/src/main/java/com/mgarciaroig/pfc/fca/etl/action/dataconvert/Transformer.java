package com.mgarciaroig.pfc.fca.etl.action.dataconvert;


public interface Transformer {
	
	public String transform(final FieldType fieldType, final String toBeTransformed);
}
