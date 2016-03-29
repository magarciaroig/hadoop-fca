package com.mgarciaroig.pfc.fca.etl.action.dataconvert.transform;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

public interface Transformer {
	
	public String transform(final FieldType fieldType, final String toBeTransformed);
}
