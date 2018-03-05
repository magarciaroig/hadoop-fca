package com.mgarciaroig.fca.analysis.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.TreeSet;

import org.junit.Test;

public class FormalConceptBuildingKeyTest {
	
	@Test
	public void constructorTest(){
		
		String buildingAttribute = "attb1";
		
		final TreeSet<String> intent = new TreeSet<>();		
		intent.add(buildingAttribute);
		intent.add("attb2");
		
		final FormalConceptBuildingKey key = new FormalConceptBuildingKey(intent, buildingAttribute);
		
		assertEquals("Unexpected building attribute", buildingAttribute, key.getBuildingAttribute());
		
		for (final String expectedAttribute : intent){
			assertTrue("Expected attribute to be included in intent", key.attributeIncludedInIntent(expectedAttribute));
		}				
	}	
	
	@Test
	public void equalObjectsTest(){
		
		String firstAttribute = "attb1";
		String secondAttribute = "attb2";
		
		final TreeSet<String> intent = new TreeSet<>();		
		intent.add(firstAttribute);		
		intent.add(secondAttribute);
		
		final FormalConceptBuildingKey firstKey = new FormalConceptBuildingKey(intent, firstAttribute);
		final FormalConceptBuildingKey secondKey = new FormalConceptBuildingKey(new TreeSet<String>(intent), new String(firstAttribute));
		
		assertTrue("Expected equal keys", firstKey.equals(secondKey));		
	}
	
	@Test
	public void distinctObjectsTest(){
		
		String firstAttribute = "attb1";
		String secondAttribute = "attb2";
		
		final TreeSet<String> intent = new TreeSet<>();		
		intent.add(firstAttribute);		
		intent.add(secondAttribute);
		
		final FormalConceptBuildingKey firstKey = new FormalConceptBuildingKey(intent, firstAttribute);
		final FormalConceptBuildingKey secondKey = new FormalConceptBuildingKey(new TreeSet<String>(intent), new String(secondAttribute));
		
		assertFalse("Expected distinct keys", firstKey.equals(secondKey));		
	}
	
	@Test
	public void compareToTest(){
		
		String firstAttribute = "attb1";
		String secondAttribute = "attb2";
		
		final TreeSet<String> intent = new TreeSet<>();		
		intent.add(firstAttribute);		
		intent.add(secondAttribute);
		
		final FormalConceptBuildingKey firstKey = new FormalConceptBuildingKey(intent, firstAttribute);
		final FormalConceptBuildingKey secondKey = new FormalConceptBuildingKey(new TreeSet<String>(intent), new String(secondAttribute));
		
		assertTrue("Expected first key lower to second key", firstKey.compareTo(secondKey) < 0);
		assertTrue("Expected second key greater to first key", secondKey.compareTo(firstKey) > 0);
		
		assertTrue("Expected equal keys", firstKey.compareTo(firstKey) == 0);
	}
}
