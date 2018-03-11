package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import com.mgarciaroig.fca.analysis.model.FormalConcept;
import com.mgarciaroig.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.fca.analysis.model.FormalConceptNonConsistentAttributesException;

import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests for formal concept Reduce function. I'm following the examples (with inputs and expected outputs) provided by Petr Krajca and Vilem Vychodil
 * in the paper "Distributed Algorithm for Computing Formal Concepts Using Map-Reduce Framework"
 *   
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class FormalConceptBuildingReducerTest extends FormalContextBuildingBaseTest {
	
	private ReduceDriver<FormalConceptBuildingKey,FormalConcept,FormalConceptBuildingKey,FormalConcept> reduceDriver;	
	
	@Before
	public void setUp() throws IOException{
		reduceDriver = new ReduceDriver<>(new FormalConceptBuildingReducerMocked(mockRepository()));
	}
	
	@Test
	public void reducePassingCanocicityTest() throws FormalConceptNonConsistentAttributesException, IOException {
		
		final FormalConceptBuildingKey inputKey = new FormalConceptBuildingKey(new TreeSet<String>(Arrays.asList("2")), "4");
		final FormalConcept inputValue = super.c14FormalConcept();
		
		final Pair<FormalConceptBuildingKey,List<FormalConcept>> reduceInput = new Pair<>(inputKey, Arrays.asList(new FormalConcept[]{inputValue}));
		
		final FormalConceptBuildingKey expectedKey = new FormalConceptBuildingKey(new TreeSet<String>(Arrays.asList("2")), "5");
		final FormalConcept expectedValue = super.c14FormalConcept();
		
		checkReduceExecution(reduceInput, new FormalConceptBuildingKey[]{expectedKey}, new FormalConcept[]{expectedValue});				
	}
	
	@Test
	public void reduceNotPassingCanocicityTest() throws FormalConceptNonConsistentAttributesException, IOException {
		
		final FormalConceptBuildingKey inputKey = new FormalConceptBuildingKey(new TreeSet<String>(Arrays.asList("2")), "3");
		final FormalConcept inputValue = super.c6FormalConcept();
		
		int expectedResults = 0;
		
		final Pair<FormalConceptBuildingKey,List<FormalConcept>> reduceInput = new Pair<>(inputKey, Arrays.asList(new FormalConcept[]{inputValue}));		
		
		reduceDriver.addInput(reduceInput);
		
		final List<Pair<FormalConceptBuildingKey, FormalConcept>> results = reduceDriver.run();			
		
		assertEquals("Expected empty reducer result", expectedResults, results.size());
	}	
	
	private void checkReduceExecution(final Pair<FormalConceptBuildingKey,List<FormalConcept>> reduceInput, 
			final FormalConceptBuildingKey[] expectedKeys, final FormalConcept[] expectedConcepts) throws IOException
	{
		reduceDriver.addInput(reduceInput);
		
		final List<Pair<FormalConceptBuildingKey, FormalConcept>> results = reduceDriver.run();
		
		int currentKeyIndex = 0;
		for (final FormalConcept currentExpectedResult : expectedConcepts){
			
			final FormalConceptBuildingKey currentExpectedKey = expectedKeys[currentKeyIndex++];
									
			final String assertionErrorMsg = String.format("Result was not found: '%s' : '%s'", currentExpectedKey.toString(), currentExpectedResult.toString());
						
			assertTrue(assertionErrorMsg, validateResult(results, currentExpectedKey, currentExpectedResult));
		}
	}
}
