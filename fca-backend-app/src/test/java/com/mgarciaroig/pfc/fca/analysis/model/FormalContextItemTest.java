package com.mgarciaroig.pfc.fca.analysis.model;

import static org.junit.Assert.*;

import java.util.List;
import java.util.TreeMap;

import org.junit.Test;

public class FormalContextItemTest {
	
	@Test
	public void testIdentifier(){
		
		final String expectedId = "id";
		
		final FormalContextItem formalContextItem = new FormalContextItem(expectedId, new TreeMap<String,Boolean>());
		
		final String obtainedId = formalContextItem.getObjectId();
		
		assertEquals("Unexpected identifier", expectedId, obtainedId);		
	}
	
	@Test
	public void testGetAttbNamesInOrder(){
	
		final String firstAttb = "A";
		final String secondAttb = "B";
		
		final TreeMap<String,Boolean> attbs = new TreeMap<String,Boolean>();
		
		attbs.put(secondAttb, true);		
		attbs.put(firstAttb, false);
		
		final FormalContextItem formalContextItem = new FormalContextItem("id", attbs);
		final List<String> attbNames = formalContextItem.allAttbNamesInOrder();		
		
		assertEquals("Expected first attribute", firstAttb, attbNames.get(0));
		assertEquals("Expected second attribute", secondAttb, attbNames.get(1));
	}

}
