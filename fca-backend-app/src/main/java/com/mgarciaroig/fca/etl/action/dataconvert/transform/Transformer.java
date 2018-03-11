package com.mgarciaroig.fca.etl.action.dataconvert.transform;

import com.mgarciaroig.fca.etl.action.dataconvert.FieldType;

public interface Transformer {
	
	public String transform(final FieldType fieldType, final String toBeTransformed);
}
