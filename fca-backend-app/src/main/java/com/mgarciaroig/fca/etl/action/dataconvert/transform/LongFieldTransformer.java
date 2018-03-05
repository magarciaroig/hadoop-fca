package com.mgarciaroig.fca.etl.action.dataconvert.transform;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

public class LongFieldTransformer extends BaseRegexTransformer {
	
	private static final String LONG_REGEX = "[-+]?\\d+";
	
	public LongFieldTransformer() {
		super(LONG_REGEX, FieldType.LONG_FIELD);
	}	
}
