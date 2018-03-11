package com.mgarciaroig.fca.analysis.persistence;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.mgarciaroig.fca.analysis.model.FormalContextItem;

public class FormalContextItemBuilderTest {
	
	final FormalContextItemBuilder builder = new FormalContextItemBuilder();
	
	@Test
	public void testBuildFromNonEmptyRawAttbs(){	
						
		final String aFormalConceptId = "id";
		
		final SortedMapWritable formalConceptAttbs = buildRawAttbs(false, true);
				
		final FormalContextItem concept = builder.build(aFormalConceptId, formalConceptAttbs);
		
		final String expectedId = aFormalConceptId;
		final String obtainedId = concept.getObjectId();
		
		assertFalse("Expected attb1 to be false", concept.hasAttb(buildAttbName(1)));
		assertTrue("Expected attb2 to be true", concept.hasAttb(buildAttbName(2)));
		assertEquals("Unexpected formal concept id", expectedId, obtainedId);
	}
	
	@Test
	public void testBuildFromEmptyRawAttbs(){	
						
		final String aFormalConceptId = "id";
		
		final SortedMapWritable formalConceptAttbs = new SortedMapWritable();
				
		final FormalContextItem concept = builder.build(aFormalConceptId, formalConceptAttbs);
		
		final String expectedId = aFormalConceptId;
		final String obtainedId = concept.getObjectId();
		
		assertFalse("Expected attb1 to be false", concept.hasAttb(buildAttbName(1)));
		assertFalse("Expected attb2 to be false", concept.hasAttb(buildAttbName(2)));
		assertEquals("Unexpected formal concept id", expectedId, obtainedId);
	}
	
	private SortedMapWritable buildRawAttbs(final boolean... values){
		
		final SortedMapWritable formalConceptAttbs = new SortedMapWritable();		
		
		int attbNumber = 1;
		for (boolean currentValue : values){
			
			formalConceptAttbs.put(new Text(buildAttbName(attbNumber)), new BooleanWritable(currentValue));
			attbNumber++;
		}
		
		return formalConceptAttbs;		
	}

	private String buildAttbName(int attbNumber) {
		return "attb".concat(String.valueOf(attbNumber));
	}
}
