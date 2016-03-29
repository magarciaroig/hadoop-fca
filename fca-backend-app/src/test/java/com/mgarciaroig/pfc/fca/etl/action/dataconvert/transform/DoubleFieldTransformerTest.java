package com.mgarciaroig.pfc.fca.etl.action.dataconvert.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

public class DoubleFieldTransformerTest {
	
	private final DoubleFieldTransformer transformer = new DoubleFieldTransformer();

	@Test
	public void testTransformValidDoubleText(){
		
		final String validDoubleTextInput = "-5.0";
		
		final String expectedDoubleText = validDoubleTextInput;			
		final String obtainedDoubleText = transformer.transform(FieldType.DOUBLE_FIELD, validDoubleTextInput);
		
		assertEquals("Expected valid Double text", expectedDoubleText, obtainedDoubleText);		
	}
	
	
	@Test
	public void testTransformInvalidDoubleText(){
		
		final String invalidDoubleTextInput = "non Double";
				
		final String obtainedDoubleText = transformer.transform(FieldType.DOUBLE_FIELD, invalidDoubleTextInput);
		
		assertNull("Expected invalid Double text", obtainedDoubleText);		
	}
	
	@Test
	public void testTransformSemiValidDoubleText(){
		
		final String validDoubleText = "10.2";
		final String invalidDoubleText = "non Double";
		
		final String semiValidDoubleTextInput = invalidDoubleText.concat(validDoubleText);
		
		final String expectedDoubleText = validDoubleText;
		final String obtainedDoubleText = transformer.transform(FieldType.DOUBLE_FIELD, semiValidDoubleTextInput);
		
		assertEquals("Expected valid Double text", expectedDoubleText, obtainedDoubleText);		
	}
	
	
	@Test
	public void testTransformValidDoubleWithoutDecimalsText(){
		
		final String validDoubleTextInput = "5";
		
		final String expectedDoubleText = validDoubleTextInput;	
		final String obtainedDoubleText = transformer.transform(FieldType.DOUBLE_FIELD, validDoubleTextInput);
		
		assertEquals("Expected valid Double text", expectedDoubleText, obtainedDoubleText);		
	}
	
	@Test
	public void testTransformValidDoubleWithScientificNotation(){
		
		final String validDoubleTextInput = "-2.5E14";
		
		final String expectedDoubleText = validDoubleTextInput;	
		final String obtainedDoubleText = transformer.transform(FieldType.DOUBLE_FIELD, validDoubleTextInput);
		
		assertEquals("Expected valid Double text", expectedDoubleText, obtainedDoubleText);		
	}
	
	
}
