package com.mgarciaroig.pfc.fca.etl.action.dataconvert.transform;

import static org.junit.Assert.*;

import org.junit.Test;
import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;

public class LongFieldTransformerTest {
	
	private final LongFieldTransformer transformer = new LongFieldTransformer();
	
	@Test
	public void testTransformValidLongText(){
		
		final String validLongTextInput = "-5";
		
		final String expectedLongText = validLongTextInput;	
		final String obtainedLongText = transformer.transform(FieldType.LONG_FIELD, validLongTextInput);
		
		assertEquals("Expected valid Long text", expectedLongText, obtainedLongText);		
	}
	
	
	@Test
	public void testTransformInvalidLongText(){
		
		final String invalidLongTextInput = "non Long";
				
		final String obtainedLongText = transformer.transform(FieldType.LONG_FIELD, invalidLongTextInput);
		
		assertNull("Expected invalid Long text", obtainedLongText);		
	}
	
	@Test
	public void testTransformSemiValidLongText(){
		
		final String validLongText = "10";
		final String invalidLongText = "non Long";
		
		final String semiValidLongTextInput = invalidLongText.concat(validLongText);
		
		final String expectedLongText = validLongText;
		final String obtainedLongText = transformer.transform(FieldType.LONG_FIELD, semiValidLongTextInput);
		
		assertEquals("Expected valid Long text", expectedLongText, obtainedLongText);		
	}	
}
