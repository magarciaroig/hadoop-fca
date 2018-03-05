package com.mgarciaroig.fca.analysis.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

import org.junit.Test;

import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

public class FormalConceptTest {
	
	private final String firstSharedAttb = "attb_shared_a";
	private final String secondSharedAttb = "attb_shared_b";
	
	private final String nonSharedAttb = "attb_non_shared";
		
	private final List<String> sharedAttributes = Arrays.asList(new String[]{firstSharedAttb, secondSharedAttb});	
	
	@Test
	public void constructorByObjectsAndAttributesTest() throws FormalConceptNonConsistentAttributesException{
		
		final String idFirstObject = "id1"; 
		final String idSecondObject = "id2";
		
		final List<String> expectedObjects = Arrays.asList(new String[]{idFirstObject, idSecondObject});
		
		final FormalContextItem firstObject = buildFormalContextItem(idFirstObject, sharedAttributes, nonSharedAttb, true);
		
		final FormalContextItem secondObject = buildFormalContextItem(idSecondObject, sharedAttributes, nonSharedAttb, false);		
		
		final List<FormalContextItem> objects = Arrays.asList(new FormalContextItem[]{firstObject, secondObject});
		
		final FormalConcept formalConcept = new  FormalConcept(objects, sharedAttributes);
		
		checkSharedAttributes(formalConcept);
		checkObjects(expectedObjects, formalConcept);
	}
	
	@Test(expected=FormalConceptNonConsistentAttributesException.class)
	public void constructorByObjectsAndAttributesInconsistentTest() throws FormalConceptNonConsistentAttributesException{
		
		final String idFirstObject = "id1"; 
		final String idSecondObject = "id2";
		
		final List<String> expectedObjects = Arrays.asList(new String[]{idFirstObject, idSecondObject});
									
		final FormalContextItem firstObject = buildFormalContextItem(idFirstObject, sharedAttributes, nonSharedAttb, true);
					
		final FormalContextItem secondObject = buildFormalContextItem(idSecondObject, sharedAttributes, nonSharedAttb, false);
		
		final List<FormalContextItem> objects = Arrays.asList(new FormalContextItem[]{firstObject, secondObject});
		
		final List<String> nonSharedByAllAttbs = new ArrayList<>(sharedAttributes);
		nonSharedByAllAttbs.add(nonSharedAttb);
		
		final FormalConcept formalConcept = new  FormalConcept(objects, nonSharedByAllAttbs);
		
		checkSharedAttributes(formalConcept);
		checkObjects(expectedObjects, formalConcept);
	}
	
	@Test
	public void constructorByObjectsTest() throws FormalConceptNonConsistentAttributesException{						
	
		final String idFirstObject = "id1"; 
		final String idSecondObject = "id2";
		
		final List<String> expectedObjects = Arrays.asList(new String[]{idFirstObject, idSecondObject});
									
		final FormalContextItem firstObject = buildFormalContextItem(idFirstObject, sharedAttributes, nonSharedAttb, true);
					
		final FormalContextItem secondObject = buildFormalContextItem(idSecondObject, sharedAttributes, nonSharedAttb, false);
		
		final List<FormalContextItem> objects = Arrays.asList(new FormalContextItem[]{firstObject, secondObject});
		
		final FormalConcept formalConcept = new  FormalConcept(objects);
		
		checkSharedAttributes(formalConcept);
		
		assertFalse("Unexpected non coommon attribute in the formal concept", formalConcept.hasAttb(nonSharedAttb));
		
		checkObjects(expectedObjects, formalConcept);		
	}
	
	@Test
	public void derivateNewFormalConceptTest() throws FormalConceptNonConsistentAttributesException, IOException{
		
		String firstAttb = "attb1";
		String secondAttb = "attb2";
		String thirdAttb = "attb3";
		String fourthAttb = "attb4";
		
		String firstObjectId = "id1";
		String secondObjectId = "id2";
		String thirdObjectId = "id3";		
		String fourthObjectId = "id4";
		
		final TreeMap<String, Boolean> attbsObject1 = buildAtributesMap(new String[]{firstAttb, secondAttb, thirdAttb});
		final TreeMap<String, Boolean> attbsObject2 = buildAtributesMap(new String[]{firstAttb, secondAttb});				
		final TreeMap<String, Boolean> attbsObject3 = buildAtributesMap(new String[]{firstAttb, secondAttb, thirdAttb, fourthAttb});
				
		final FormalContextItem object1 = new FormalContextItem(firstObjectId, attbsObject1);		
		final FormalContextItem object2 = new FormalContextItem(secondObjectId, attbsObject2);		
		final FormalContextItem object3 = new FormalContextItem(thirdObjectId, attbsObject3);
		
		final List<FormalContextItem> sourceObjects = Arrays.asList(new FormalContextItem[]{object1, object2, object3});
		
		final FormalConcept source = new FormalConcept(sourceObjects);
		
		final List<String> objectIdsForThirdAttb = Arrays.asList(new String[]{firstObjectId, thirdObjectId});
		final List<FormalContextItem> objectsForThirdAttb = Arrays.asList(new FormalContextItem[]{object1, object3});
		
		final FormalContextItemRepository repositoryForAddindThirdAttb = mockFindObjectsByAttbRepository(thirdAttb, objectsForThirdAttb, objectIdsForThirdAttb);		
		
		final FormalConcept target = source.deriveNewFormalConcept(repositoryForAddindThirdAttb, thirdAttb);		
		
		assertTrue("Expected object id in the formal context", target.hasObject(firstObjectId));
		assertTrue("Expected object id in the formal context", target.hasObject(thirdObjectId));
		
		assertFalse("Unxpected object id in the formal context", target.hasObject(secondObjectId));		
		assertFalse("Unxpected object id in the formal context", target.hasObject(fourthObjectId));
		
		assertTrue("Expected attribute in the formal context", target.hasAttb(firstAttb));
		assertTrue("Expected attribute in the formal context", target.hasAttb(secondAttb));
		assertTrue("Expected attribute in the formal context", target.hasAttb(thirdAttb));
		
		assertFalse("Unexpected attribute in the formal context", target.hasAttb(fourthAttb));
	}
	
	@Test
	public void derivateNewKeyTest() throws FormalConceptNonConsistentAttributesException{
		
		String firstAttb = "attb1";
		String secondAttb = "attb2";		
		
		String expectedKeyBuildingAttribute = "attb3";
		
		final String[] expectedAttributesInKeyIntent = {firstAttb, secondAttb};
		
		final TreeMap<String, Boolean> atributes = buildAtributesMap(new String[]{firstAttb, secondAttb});
		
		final FormalContextItem object = new FormalContextItem("id", atributes);
		
		FormalConcept concept = new  FormalConcept(Arrays.asList(new FormalContextItem[]{object}));
		final FormalConceptBuildingKey derivatedKey = concept.deriveNewBuildingKey(expectedKeyBuildingAttribute);
		
		for (final String currentExpectedAttb : expectedAttributesInKeyIntent){
			
			assertTrue("Expected attribute not found in key intent", derivatedKey.attributeIncludedInIntent(currentExpectedAttb));
		}			
		
		assertEquals("Unexpected building attribute in key", expectedKeyBuildingAttribute, derivatedKey.getBuildingAttribute());
	}
	
	@Test
	public void equalObjectsTest() throws FormalConceptNonConsistentAttributesException{
	
		String firstAttb = "attb1";
		String secondAttb = "attb2";
				
		String firstObjectId = "id1";
		String secondObjectId = "id2";		
		
		final TreeMap<String, Boolean> attbsObject1 = buildAtributesMap(new String[]{firstAttb, secondAttb});
		final TreeMap<String, Boolean> attbsObject2 = buildAtributesMap(new String[]{firstAttb, secondAttb});								
				
		final FormalContextItem object1 = new FormalContextItem(firstObjectId, attbsObject1);
		final FormalContextItem object2 = new FormalContextItem(secondObjectId, attbsObject2);
		
		final FormalConcept firstConcept = new FormalConcept(Arrays.asList(new FormalContextItem[]{object1, object2}));
		final FormalConcept secondConcept = new FormalConcept(Arrays.asList(new FormalContextItem[]{object1, object2}));
		
		assertTrue("Expected equal concepts", firstConcept.equals(secondConcept));				
	}
	
	@Test
	public void distinctObjectsTest() throws FormalConceptNonConsistentAttributesException{
	
		String firstAttb = "attb1";
		String secondAttb = "attb2";
				
		String firstObjectId = "id1";
		String secondObjectId = "id2";		
		
		final TreeMap<String, Boolean> attbsObject1 = buildAtributesMap(new String[]{firstAttb, secondAttb});
		final TreeMap<String, Boolean> attbsObject2 = buildAtributesMap(new String[]{firstAttb, secondAttb});								
				
		final FormalContextItem object1 = new FormalContextItem(firstObjectId, attbsObject1);
		final FormalContextItem object2 = new FormalContextItem(secondObjectId, attbsObject2);
		
		final FormalConcept firstConcept = new FormalConcept(Arrays.asList(new FormalContextItem[]{object1}));
		final FormalConcept secondConcept = new FormalConcept(Arrays.asList(new FormalContextItem[]{object2}));
		
		assertFalse("Expected equal concepts", firstConcept.equals(secondConcept));				
	}
	
	private FormalContextItemRepository mockFindObjectsByAttbRepository(final String attb, final List<FormalContextItem> expectedObjectList, final List<String> expectedObjectsId) throws IOException{
		
		final FormalContextItemRepository repository = mock(FormalContextItemRepository.class);
		
		when(repository.find(any(Collection.class))).thenReturn(expectedObjectList);
		
		when(repository.findObjectIdsByAttribute(attb)).thenReturn(expectedObjectsId);				
		
		return repository;
	}

	private void checkObjects(final List<String> expectedObjects,
			final FormalConcept formalConcept) {
		for (final String currentExpectedObjectId : expectedObjects){
			assertTrue("An expected object was not found in the formal concept", formalConcept.hasObject(currentExpectedObjectId));
		}
	}

	private void checkSharedAttributes(final FormalConcept formalConcept) {
		for (final String currentSharedAttb : sharedAttributes){
			assertTrue("Expected formal concept to have a common attribute", formalConcept.hasAttb(currentSharedAttb));
		}
	}
	
	private FormalContextItem buildFormalContextItem(final String objectId, final List<String> sharedAttributes, final String nonSharedAttbName, final boolean nonSharedAttbValue){
		
		final TreeMap<String,Boolean> attbsMap = buildSharedPlusNonSharedAtributesMap(sharedAttributes, nonSharedAttbName, nonSharedAttbValue);
		
		return new FormalContextItem(objectId, attbsMap);
	}
	
	private TreeMap<String,Boolean> buildAtributesMap(final String[] attributes){
		
		final TreeMap<String,Boolean> attbsMap = new TreeMap<>();
		
		for (int currentAttbIndex = 0; currentAttbIndex < attributes.length; currentAttbIndex++){
			
			attbsMap.put(attributes[currentAttbIndex], true);
		}			
		
		return attbsMap;
	}
	
	private TreeMap<String,Boolean> buildSharedPlusNonSharedAtributesMap(final List<String> sharedAttributes, final String nonSharedAttbName, final boolean nonSharedAttbValue){
		
		final TreeMap<String,Boolean> attbsMap = buildSharedAttributesMap(sharedAttributes);
		attbsMap.put(nonSharedAttbName, nonSharedAttbValue);
		
		return attbsMap;
	}
	
	private TreeMap<String,Boolean> buildSharedAttributesMap(final List<String> sharedAttributes){
		
		final TreeMap<String,Boolean> attbs = new TreeMap<>();
		for (final String currentSharedAttb : sharedAttributes){
			attbs.put(currentSharedAttb, true);
		}
		
		return attbs;
	}
}
