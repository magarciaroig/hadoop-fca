package com.mgarciaroig.pfc.fca.analysis.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class AttributesUtilTest {
		
	private static final String FIRST_ATTB = "0";
	private static final String NEXT_FROM_FIRST_ATTB = "1";
	private static final String LAST_ATTB = "4";
	
	private static final String INTERMEDIATE_ATTB = "2";
	private static final String NEXT_FROM_INTERMEDIATE_ATTB = "3";
	
	private List<String> allAttbsInOrder = Arrays.asList(new String[]{FIRST_ATTB, NEXT_FROM_FIRST_ATTB, INTERMEDIATE_ATTB, NEXT_FROM_INTERMEDIATE_ATTB, LAST_ATTB});
	
	private AttributesUtil util = new AttributesUtil(allAttbsInOrder);		
	
	@Test
	public void attributesFromFirstAttbTest(){
		
		final List<String> expectedAttbs = allAttbsInOrder;
		final List<String> obtainedAttbs = util.attributesFrom(FIRST_ATTB); 
	
		checkResult(expectedAttbs, obtainedAttbs);		
	}
	
	@Test
	public void attributesFromIntermediateAttbTest(){
		
		final List<String> expectedAttbs = allAttbsInOrder.subList(allAttbsInOrder.indexOf(INTERMEDIATE_ATTB),allAttbsInOrder.size());
		final List<String> obtainedAttbs = util.attributesFrom(INTERMEDIATE_ATTB);
		
		checkResult(expectedAttbs, obtainedAttbs);		
	}
	
	@Test
	public void attributesFromLastAttbTest(){
		final List<String> expectedAttbs = Arrays.asList(new String[]{LAST_ATTB});
		final List<String> obtainedAttbs = util.attributesFrom(LAST_ATTB);
		
		checkResult(expectedAttbs, obtainedAttbs);		
	}
	
	@Test
	public void attributesFromNonExistentAttbTest(){
		
		final List<String> expectedAttbs = new ArrayList<>();
		final List<String> obtainedAttbs = util.attributesFrom("NON_EXISTENT");
		
		checkResult(expectedAttbs, obtainedAttbs);		
	}
	
	@Test
	public void nextAttributeFromFirstAttbTest(){
	
		String expectedAttb = NEXT_FROM_FIRST_ATTB;
		final String obtainedAttb = util.nextAttribute(FIRST_ATTB);
		
		assertEquals("Unexpected next attribute was found", expectedAttb, obtainedAttb);
	}
	
	@Test
	public void nextAttributeFromIntermediateAttbTest(){
		
		String expectedAttb = NEXT_FROM_INTERMEDIATE_ATTB;
		final String obtainedAttb = util.nextAttribute(INTERMEDIATE_ATTB);
		
		assertEquals("Unexpected next attribute was found", expectedAttb, obtainedAttb);		
	}
	
	@Test
	public void nextAttributeFromEndAttbTest(){
				
		final String obtainedAttb = util.nextAttribute(LAST_ATTB);
		
		assertEquals("Expected the same attribute", LAST_ATTB, obtainedAttb);				
	}
	
	@Test
	public void nextAttributeFromNonExistentAttbTest(){
		
		final String obtainedAttb = util.nextAttribute("NON_EXISTENT");
		
		assertNull("Expected empty next attribute", obtainedAttb);
	}
		
	private void checkResult(final List<String> expectedAttbs, final List<String> obtainedAttbs){
		
		assertEquals("Unepected attributes length", expectedAttbs.size(), obtainedAttbs.size());
		
		int currentIndex = 0;
		for (final String currentExpectedAttb : expectedAttbs){
			
			final String currentObtainedAttb = obtainedAttbs.get(currentIndex++);
			
			assertEquals("Unexpected attribute", currentExpectedAttb, currentObtainedAttb);			
		}
	}
}
