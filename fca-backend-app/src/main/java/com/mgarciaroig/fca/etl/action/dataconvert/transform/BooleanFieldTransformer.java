package com.mgarciaroig.fca.etl.action.dataconvert.transform;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

public class BooleanFieldTransformer extends BaseRegexTransformer {
	
	private static final String BOOLEAN_REGEX = "yes|no";

	protected BooleanFieldTransformer() {
		super(BOOLEAN_REGEX, FieldType.BOOLEAN_FIELD);		
	}	
}
