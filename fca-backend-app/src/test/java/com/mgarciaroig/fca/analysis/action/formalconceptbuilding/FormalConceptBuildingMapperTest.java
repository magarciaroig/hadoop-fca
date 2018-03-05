package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptNonConsistentAttributesException;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for formal concept Map function. I'm following the examples (with inputs and expected outputs) provided by Petr Krajca and Vilem Vychodil
 * in the paper "Distributed Algorithm for Computing Formal Concepts Using Map-Reduce Framework"
 *   
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class FormalConceptBuildingMapperTest extends FormalContextBuildingBaseTest {		
	
	private MapDriver<FormalConceptBuildingKey,FormalConcept,FormalConceptBuildingKey,FormalConcept> mapDriver;
	
	@Before
	public void setUp() throws IOException{
		mapDriver = new MapDriver<>(new FormalConceptBuildingMapperMocked(mockRepository()));
	}
	
	/**
	 * 
	 * Test for initial execution <<EMPTY,1>, C1>
	 */
	@Test
	public void mapTestInitalExecution() throws IOException, FormalConceptNonConsistentAttributesException {
		
		final FormalConceptBuildingKey inputKey = createInitialExecutionKey();
		
		final FormalConcept inputValue = createMaximumFormalConcept();
		
		final Pair<FormalConceptBuildingKey, FormalConcept> mapInput = new Pair<>(inputKey,inputValue);
		
		checkMapExecution(mapInput, createExpectedGeneratedKeysForInitialConcept(), createExpectedGeneratedConceptsForInitialConcept());							
	}	
	
	/**
	 * 
	 * Test for intermediate execution <<0,1>, C2>
	 */
	@Test
	public void mapTestIntermediateExecution() throws IOException, FormalConceptNonConsistentAttributesException{
		
		
		final FormalConceptBuildingKey inputKey = createItermediateExecutionKey();
		
		final FormalConcept inputValue = c2FormalConcept();
		
		final Pair<FormalConceptBuildingKey, FormalConcept> mapInput = new Pair<>(inputKey,inputValue);
		
		checkMapExecution(mapInput, createExpectedGeneratedKeysForC2Concept(), createExpectedGeneratedConceptsForC2Concept());													
	}
	
	private void checkMapExecution(final Pair<FormalConceptBuildingKey,FormalConcept> mapInput, 
			final FormalConceptBuildingKey[] expectedKeys, final FormalConcept[] expectedConcepts) throws IOException
	{
		
		mapDriver.addInput(mapInput);
		
		final List<Pair<FormalConceptBuildingKey, FormalConcept>> results = mapDriver.run();
		
		int currentKeyIndex = 0;
		for (final FormalConcept currentExpectedResult : expectedConcepts){
			
			final FormalConceptBuildingKey currentExpectedKey = expectedKeys[currentKeyIndex++];
									
			final String assertionErrorMsg = String.format("Result was not found: '%s' : '%s'", currentExpectedKey.toString(), currentExpectedResult.toString());
						
			assertTrue(assertionErrorMsg, validateResult(results, currentExpectedKey, currentExpectedResult));
		}	
	}					
	
	/**
	 * 
	 * @return The expected map output keys for initial execution <<EMPTY,1>, C1>
	 */
	private FormalConceptBuildingKey[] createExpectedGeneratedKeysForInitialConcept(){
		
		return new FormalConceptBuildingKey[] {
			new FormalConceptBuildingKey(new TreeSet<String>(),"0"), //key for C2
			new FormalConceptBuildingKey(new TreeSet<String>(),"1"), //key for C11
			new FormalConceptBuildingKey(new TreeSet<String>(),"2"), //key for C13
			new FormalConceptBuildingKey(new TreeSet<String>(),"3"), //key for C6
			new FormalConceptBuildingKey(new TreeSet<String>(),"4"), //key for C14
			new FormalConceptBuildingKey(new TreeSet<String>(),"5"), //key for C15
			new FormalConceptBuildingKey(new TreeSet<String>(),"6")  //key for C16
		};				
	}
	
	/**
	 * 
	 * @return The expected map output values for initial execution <<EMPTY,1>, C1>
	 * @throws FormalConceptNonConsistentAttributesException
	 */
	private FormalConcept[] createExpectedGeneratedConceptsForInitialConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return new FormalConcept[] {
				c2FormalConcept(),
				c11FormalConcept(),
				c13FormalConcept(),
				c6FormalConcept(),
				c14FormalConcept(),
				c15FormalConcept(),
				c16FormalConcept()};								
	}		
	
	/**
	 * 
	 * @return The expected map output keys for intermediate execution <<0,1>, C2>
	 */
	private FormalConceptBuildingKey[] createExpectedGeneratedKeysForC2Concept(){
		
		final TreeSet<String> intent = new TreeSet<String>(Arrays.asList(new String[]{"0"}));
		
		return new FormalConceptBuildingKey[] {
			new FormalConceptBuildingKey(intent,"1"), //key for C3
			new FormalConceptBuildingKey(intent,"2"), //key for C5
			new FormalConceptBuildingKey(intent,"3"), //key for C6
			new FormalConceptBuildingKey(intent,"4"), //key for C9
			new FormalConceptBuildingKey(intent,"5"), //key for C7
			new FormalConceptBuildingKey(intent,"6")  //key for C10			
		};				
	}
	
	/**
	 * 
	 * @return The expected map output values for intermediate execution <<0,1>, C2>
	 * @throws FormalConceptNonConsistentAttributesException
	 */
	private FormalConcept[] createExpectedGeneratedConceptsForC2Concept()
			throws FormalConceptNonConsistentAttributesException {
		
		return new FormalConcept[] {
			c3FormalConcept(),
			c5FormalConcept(),
			c6FormalConcept(),
			c9FormalConcept(),
			c7FormalConcept(),
			c10FormalConcept()
		};								
	}
	
	protected FormalConceptBuildingKey createItermediateExecutionKey() {
		
		return new FormalConceptBuildingKey(new TreeSet<String>(), "1");
	} 
}
