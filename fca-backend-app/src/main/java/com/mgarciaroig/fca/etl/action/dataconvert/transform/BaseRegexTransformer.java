package com.mgarciaroig.fca.etl.action.dataconvert.transform;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

abstract class BaseRegexTransformer implements Transformer {
	
	private final Pattern pattern; 
	private final FieldType fieldType;
	
	protected BaseRegexTransformer(final String regex, final FieldType fieldType){
		
		pattern = Pattern.compile(regex);
		this.fieldType = fieldType;
	}
	
	@Override
	public String transform(final FieldType fieldType, final String toBeTransformed) {
		
		String transformed = toBeTransformed;
		
		if (fieldType.equals(this.fieldType)){
			transformed = findMatchedContent(toBeTransformed);
		}
		
		return transformed;
	}
	
	protected String findMatchedContent(final String textToFindIn){
		
		String foundText = null; 
		
		final Matcher matcher = pattern.matcher(textToFindIn);
		if (matcher.find()){
			foundText = matcher.group();
		}
		
		return foundText;
	}			
}
