package com.mgarciaroig.pfc.fca.etl.action.dataconvert.transform;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

public class BooleanTrasformerTest {
	
	private final BooleanFieldTransformer transformer = new BooleanFieldTransformer();

	@Test
	public void testTransformTrueValue(){
		
		final String validTrueTextInput = "dedeyesdedede";
		
		final String expectedDoubleText = "yes";	
		final String obtainedDoubleText = transformer.transform(FieldType.BOOLEAN_FIELD, validTrueTextInput);
		
		assertEquals("Expected 'true' text", expectedDoubleText, obtainedDoubleText);
	}
	
	public void testTransformFalseValue(){
		
	}
}
