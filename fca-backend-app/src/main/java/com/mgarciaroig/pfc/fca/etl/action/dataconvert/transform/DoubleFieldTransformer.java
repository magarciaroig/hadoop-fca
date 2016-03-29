package com.mgarciaroig.pfc.fca.etl.action.dataconvert.transform;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

public class DoubleFieldTransformer extends BaseRegexTransformer {
	
	private static final String DOUBLE_REGEX = "[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?";
		
	public DoubleFieldTransformer() {
		super(DOUBLE_REGEX, FieldType.DOUBLE_FIELD);
	}	
}
