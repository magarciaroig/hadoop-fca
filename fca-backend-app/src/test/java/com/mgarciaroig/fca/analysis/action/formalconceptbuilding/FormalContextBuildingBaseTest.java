package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import com.mgarciaroig.pfc.fca.analysis.model.FormalContextItem;
import org.apache.hadoop.mrunit.types.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptNonConsistentAttributesException;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

/**
 * Convenience utilities to implement map-reduce formal concept tests. I'm following the examples (with inputs and expected outputs) provided by Petr Krajca and Vilem Vychodil
 * in the paper "Distributed Algorithm for Computing Formal Concepts Using Map-Reduce Framework"
 *   
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalContextBuildingBaseTest {
	
	protected final static int OBJECTS_NUMBER = 5;
	
	protected final static int ATTBS_NUMBER = 7;
	
	
	protected final boolean [][] formalContext = {
		//      0     1      2      3      4      5      6
		/*0*/{false, true,  true,  false, true,  true,  true}, 
		/*1*/{true,  false, true,  true,  false, false, true},
		/*2*/{true,  true,  false, false, false, false, false},
		/*3*/{true,  false, true,  true,  false, true,  false},
		/*4*/{true,  false, true,  false, true,  false, true}
	};
	
	private List<String> allAttributes(){
		
		final List<String> attbs = new ArrayList<>();
		
		for (int currentAttbIndex = 0; currentAttbIndex < ATTBS_NUMBER; currentAttbIndex++){
			attbs.add(String.valueOf(currentAttbIndex));
		}
		
		return attbs;
	}	
	
	protected boolean validateResult(final List<Pair<FormalConceptBuildingKey, FormalConcept>> haystack, final FormalConceptBuildingKey key, final FormalConcept value){
		
		for (final Pair<FormalConceptBuildingKey, FormalConcept> currentResult : haystack){
									
			final FormalConceptBuildingKey currentKey = currentResult.getFirst();						
			final FormalConcept currentValue = currentResult.getSecond();
			
			if (currentKey.equals(key) && currentValue.equals(value)) return true;			
		}
		
		return false;
	}
	
	protected FormalContextItemRepository mockRepository() throws IOException{
		
		final FormalContextItemRepository repository = mock(FormalContextItemRepository.class);
		
		when(repository.findAllAttbs()).thenReturn(allAttributes());
		
		when(repository.findObjectIdsByAttribute(anyString())).thenAnswer(createMockedFindObjectsIdByAttributeMethod());
		
		when(repository.find(anyString())).thenAnswer(createMockedFindMethod());
		
		when(repository.find((Collection<String>) anyObject())).thenAnswer(createMockedFindByCollectionMethod());				
		
		return repository;
	}

	private Answer<List<FormalContextItem>> createMockedFindByCollectionMethod() {
		return new Answer<List<FormalContextItem>>(){

			@Override
			public List<FormalContextItem> answer(final InvocationOnMock invocation) throws Throwable {
				
				final Collection<String> objectIds = (Collection<String>) invocation.getArguments()[0];
				
				final List<FormalContextItem> formalContextItems = new ArrayList<>();
				
				for (final String currentObjectId : objectIds){
					formalContextItems.add(createFormalContextItem(currentObjectId));
				}
				
				return formalContextItems;
			}
			
		};
	}

	private Answer<FormalContextItem> createMockedFindMethod() {
		return new Answer<FormalContextItem>(){

			@Override
			public FormalContextItem answer(final InvocationOnMock invocation) throws Throwable {
				
				final String objectId = invocation.getArguments()[0].toString();
				
				return createFormalContextItem(objectId);
			}
			
		};
	}

	private Answer<List<String>> createMockedFindObjectsIdByAttributeMethod() {
		return new Answer<List<String>>(){

			@Override
			public List<String> answer(final InvocationOnMock invocation) throws Throwable {
												
				final String attb = invocation.getArguments()[0].toString();
				final int columnNumber = Integer.valueOf(attb);
				
				final List<String> objectIds = new ArrayList<String>();
				
				for (int currentObjectId = 0; currentObjectId < formalContext.length; currentObjectId++){
					if (formalContext[currentObjectId][columnNumber]){
						objectIds.add(String.valueOf(currentObjectId));
					}
				}
				
				return objectIds;
			}
			
		};
	}		


	
	protected FormalConcept c2FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		return createFormalConcept(new String[] {"1","2","3","4"}, new String[]{"0"});
	}
	
	protected FormalConcept c3FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return createFormalConcept(new String[] {"2"}, new String[]{"0","1"});
	}
	
	protected FormalConcept c5FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return createFormalConcept(new String[] {"1","3","4"}, new String[]{"0","2"});
	}
	
	protected FormalConcept c6FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		return createFormalConcept(new String[] {"1", "3"}, new String[]{"0","2","3"});
	}
	
	protected FormalConcept c7FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return createFormalConcept(new String[] {"3"}, new String[]{"0","2","3","5"});
	}
	
	protected FormalConcept c8FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return createFormalConcept(new String[] {"1"}, new String[]{"0", "2", "3", "6"});
	}
	
	protected FormalConcept c9FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return createFormalConcept(new String[] {"4"}, new String[]{"0","2","4","6"});
	}
	
	protected FormalConcept c10FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return createFormalConcept(new String[] {"1","4"}, new String[]{"0","2","6"});
	}
	
	protected FormalConcept c11FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		return createFormalConcept(new String[] {"0","2"}, new String[]{"1"});
	}
	
	protected FormalConcept c12FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		return createFormalConcept(new String[] {"0"}, new String[]{"1", "2", "4", "5", "6"});
	}
	
	protected FormalConcept c13FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		return createFormalConcept(new String[] {"0","1","3","4"}, new String[]{"2"});
	}
	
	protected FormalConcept c14FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		return createFormalConcept(new String[] {"0","4"}, new String[]{"2","4","6"});
	}
	
	protected FormalConcept c15FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		return createFormalConcept(new String[] {"0","3"}, new String[]{"2","5"});
	}

	protected FormalConcept c16FormalConcept()
			throws FormalConceptNonConsistentAttributesException {
		
		return createFormalConcept(new String[] {"0","1","4"}, new String[]{"2","6"});
	}	
	
	protected FormalConcept createFormalConcept(final String[] objectIds, final String[] attbs) throws FormalConceptNonConsistentAttributesException{
		
		final List<FormalContextItem> allObjects = new ArrayList<>();
		
		for (final String currentobjectId : objectIds){
			allObjects.add(createFormalContextItem(currentobjectId));
		}
				
		return new FormalConcept(allObjects, Arrays.asList(attbs));			
	}
	
	protected FormalContextItem createFormalContextItem(final String objectId){
		
		int row = Integer.valueOf(objectId);
		final TreeMap<String,Boolean> attbs = new TreeMap<>();
		
		for (int attbIndex = 0; attbIndex < ATTBS_NUMBER; attbIndex++){
			
			final String attbName = String.valueOf(attbIndex);
			attbs.put(attbName, formalContext[row][attbIndex]);
		}
		
		return new FormalContextItem(objectId, attbs);				
	}
	
	protected FormalConceptBuildingKey createInitialExecutionKey() {
		
		return new FormalConceptBuildingKey(new TreeSet<String>(), "0");
	}		
	
	protected FormalConcept createMaximumFormalConcept() throws FormalConceptNonConsistentAttributesException {
					
		
		final List<String> allObjectIds = new ArrayList<>();
		
		for (int currentObjectIndex = 0; currentObjectIndex < OBJECTS_NUMBER; currentObjectIndex++){
			
			final String currentObjectId = String.valueOf(currentObjectIndex);
			
			allObjectIds.add(currentObjectId);
		}	
				
		return new FormalConcept(allObjectIds, new ArrayList<String>());		
	}			

}
