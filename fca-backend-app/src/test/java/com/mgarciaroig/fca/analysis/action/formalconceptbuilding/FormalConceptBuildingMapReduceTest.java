package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptNonConsistentAttributesException;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

/**
 * Tests for formal concept full Map-Reduce. I'm following the examples (with inputs and expected outputs) provided by Petr Krajca and Vilem Vychodil
 * in the paper "Distributed Algorithm for Computing Formal Concepts Using Map-Reduce Framework"
 *   
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalConceptBuildingMapReduceTest extends FormalContextBuildingBaseTest {

	private MapReduceDriver<FormalConceptBuildingKey,FormalConcept,FormalConceptBuildingKey,FormalConcept, FormalConceptBuildingKey,FormalConcept> mapReduceDriver;
	
	@Before
	public void setUp() throws IOException{
		initDriver();		
	}			
	
	@Test
	public void testFullExecutionUntilConverges() throws FormalConceptNonConsistentAttributesException, IOException{
		
		addMapReduceInitialInput();
		
		List<Pair<FormalConceptBuildingKey, FormalConcept>> finalResults = new ArrayList<>();
		
		List<Pair<FormalConceptBuildingKey, FormalConcept>> previousResults = executeMapReduce(finalResults);
							
		int lastResultNumber = previousResults.size();	
		
		int iterations = 1;
		int maxIterations = 50;
						
		do {						
			initDriver();			
			
			addMapReduceInput(previousResults);
						
			previousResults = executeMapReduce(finalResults);
			
			lastResultNumber = previousResults.size();			
		}
		while ((++iterations < maxIterations) && (lastResultNumber > 0));
		
		assertTrue("Expected algorythm convergence", iterations < maxIterations);
										
		final FormalConcept[] expectedFormalConcepts = {c2FormalConcept(), c3FormalConcept(), c5FormalConcept(), c6FormalConcept(), c7FormalConcept(),
				c8FormalConcept(), c9FormalConcept(), c10FormalConcept(), c11FormalConcept(), c12FormalConcept(), c13FormalConcept(), c14FormalConcept(),
				c15FormalConcept(), c16FormalConcept()};
		
		assertEquals("Unexpected number of final results", expectedFormalConcepts.length, finalResults.size());
						
						
		for (final FormalConcept currentExpectedFormalConcept : expectedFormalConcepts){
			
			final String assertionError = String.format("Expected '%s' formal concept", currentExpectedFormalConcept.toString());
			
			assertTrue(assertionError, formalConceptWasGenerated(finalResults, currentExpectedFormalConcept));
		}
	}
	
	private boolean formalConceptWasGenerated(final List<Pair<FormalConceptBuildingKey, FormalConcept>> results, final FormalConcept formalConcept){
		
		for (final Pair<FormalConceptBuildingKey, FormalConcept> currentResult : results){
			
			if (currentResult.getSecond().equals(formalConcept)) return true;
		}		
		 
		return false;
	}

	private void addMapReduceInitialInput() throws FormalConceptNonConsistentAttributesException {
		
		final FormalConceptBuildingKey inputKey = createInitialExecutionKey();
		
		final FormalConcept inputValue = createMaximumFormalConcept();
		
		mapReduceDriver.addInput(inputKey, inputValue);
	}

	private void addMapReduceInput(final List<Pair<FormalConceptBuildingKey, FormalConcept>> previousResults) {
		
		for (final Pair<FormalConceptBuildingKey, FormalConcept> input : previousResults){
			mapReduceDriver.addInput(input.getFirst(), input.getSecond());
		}
	}

	private List<Pair<FormalConceptBuildingKey, FormalConcept>> executeMapReduce(final List<Pair<FormalConceptBuildingKey, FormalConcept>> finalResults)
			throws IOException {
		
		List<Pair<FormalConceptBuildingKey, FormalConcept>> previousResults = mapReduceDriver.run();
		
		finalResults.addAll(previousResults);
		
		return previousResults;
	}
	
	private void initDriver() throws IOException{
		
		final FormalContextItemRepository repository = mockRepository();
		
		mapReduceDriver = new MapReduceDriver<>(new FormalConceptBuildingMapperMocked(repository), new FormalConceptBuildingReducerMocked(repository));
	}
}
