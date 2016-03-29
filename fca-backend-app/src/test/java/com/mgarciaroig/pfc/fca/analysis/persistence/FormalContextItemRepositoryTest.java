package com.mgarciaroig.pfc.fca.analysis.persistence;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.mgarciaroig.pfc.fca.analysis.model.FormalContextItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class FormalContextItemRepositoryTest {
	
	private FormalContextItemRepository repository;
	
	public FormalContextItemRepositoryTest() throws IOException{
		
		final String testFilesBasePathName = "./src/test/resources/com/mgarciaroig/pfc/fca/analysis/persistence";
				
		final FileSystem fs = FileSystem.get(new Configuration());		
		final Path formalContextFile = new Path(testFilesBasePathName.concat("/formal_context"));
		final Path idsByAttbPath = new Path(testFilesBasePathName);
		
		repository = new FormalContextItemRepository(fs, formalContextFile, idsByAttbPath);
	}
	
	@Test
	public void testFindAllAttbs() throws IOException{
		
		final List<String> obtainedAttbs = repository.findAllAttbs();
		
		final String obtainedFirstAttbName = obtainedAttbs.get(0);
		final String obtainedLastAttbName = obtainedAttbs.get(obtainedAttbs.size() - 1);
					
		assertEquals("Unexpected first attb", getFirstExpectedAttName(), obtainedFirstAttbName);		
		assertEquals("Unexpected last attb", getLastExpectedAttName(), obtainedLastAttbName);
	}
	
	@Test
	public void testFindAllobjectIds() throws IOException{
		
		final List<String> obtainedObjectIds = repository.findAllObjectIds();
		
		final String obtainedFirstObjectId = obtainedObjectIds.get(0);
		final String obtainedLastObjectId = obtainedObjectIds.get(obtainedObjectIds.size() - 1);
		
		assertEquals("Unexpected number of objects", getExpectedObjectIdsNumber(), obtainedObjectIds.size());		
		assertEquals("Unexpected first object", getFirstExpectedObjectId(), obtainedFirstObjectId);				
		assertEquals("Unexpected last attb", getLastExpectedObjectId(), obtainedLastObjectId);
	}
	
	@Test
	public void testFindSeveralObjects() throws IOException{
		
		final List<String> objectIds = Arrays.asList(new String[]{"0bcaaafe-ac28-4a50-b24e-ceb519f81917", "9fcab336-3a37-4e96-9b8b-16f76b0245b3"});
		
		final List<FormalContextItem> items = repository.find(objectIds);
		
		assertEquals("Unexpected number of items", objectIds.size(), items.size());
		
		for (int currentIndex = 0; currentIndex < objectIds.size(); currentIndex++){
			
			assertEquals("Unexpected object", objectIds.get(currentIndex), items.get(currentIndex).getObjectId());
		}			
	}
	
	@Test
	public void testFindOne() throws IOException{
		
		final String objectId = "8d705b70-23a2-4354-ac5b-0d20d077a8a2";
		final FormalContextItem item = repository.find(objectId);
		
		assertNotNull("Expected to find an item", item);
		assertEquals("Unexpected object was found", objectId, item.getObjectId());		
	}
	
	@Test
	public void testFindNonExistent() throws IOException{
		
		final FormalContextItem item = repository.find("NON EXISTENT");
		
		assertNull("Expected to find an item", item);
	}
	
	@Test
	public void testFindByAttb() throws IOException{
	
		final String[] expectedObjectIds = {"fad95879-098d-43a1-a3a1-9c11eaf1898b"};
		
		final List<FormalContextItem> foundItems = repository.findByAttb("Location.country-ENUM-032");
		
		assertEquals("Unexpected obtained objects by attb", expectedObjectIds.length, foundItems.size());
		
		for (int currentobjectIndex = 0; currentobjectIndex < expectedObjectIds.length; currentobjectIndex++){
			
			assertEquals("Expected to found object", expectedObjectIds[currentobjectIndex], foundItems.get(currentobjectIndex).getObjectId());
		} 
	}

	@Test
	public void findObjectIdsByAttributeTest() throws IOException{
		
		final String[] expectedIds = {"9e97983b-3604-4f60-9154-4cb5b973f186", "2495abe4-dc45-465a-b444-8dc9b30a79d2"};
		
		final List<String> foundIds =  repository.findObjectIdsByAttribute("Category.category-ENUM-006");
		
		assertEquals("Unexpected number of objects", expectedIds.length, foundIds.size());
		
		for (final String currentExpectedId : expectedIds){
			assertTrue ("Expected object id to be found", foundIds.contains(currentExpectedId));
		}
	}
	
	private String getFirstExpectedAttName(){
		return "Category.category-ENUM-000";	
	}
	
	private String getLastExpectedAttName(){
		return "Utilization.weeks_per_year-CLUSTER-UND";		
	}
	
	private int getExpectedObjectIdsNumber(){
		return 150;
	}

	private String getFirstExpectedObjectId(){
		return "05155f34-05a0-48ae-969e-7d8e6455d2a7";	
	}
	
	private String getLastExpectedObjectId(){
		return "fbe9e7e9-3c28-4bcf-a3c3-8c4113d1ce40";	
	}
		
}
