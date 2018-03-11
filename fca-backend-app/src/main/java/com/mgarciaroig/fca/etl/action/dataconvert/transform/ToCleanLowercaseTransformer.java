package com.mgarciaroig.fca.etl.action.dataconvert.transform;

import com.mgarciaroig.fca.etl.action.dataconvert.FieldType;

public class ToCleanLowercaseTransformer implements Transformer {

	public String transform(FieldType fieldType, String toBeTransformed){
		
		String transformed = null;
		
		if (toBeTransformed != null){
		
			transformed = toBeTransformed.trim().toLowerCase();
		}
		
		return transformed;
	}

}
