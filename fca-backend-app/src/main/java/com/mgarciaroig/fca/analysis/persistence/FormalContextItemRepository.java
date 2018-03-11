package com.mgarciaroig.fca.analysis.persistence;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import com.mgarciaroig.fca.analysis.model.FormalContextItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Repository class to load the formal context items from serialization
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class FormalContextItemRepository {
	
	private interface FormalContextItemProcessor {
		
		boolean process(FormalContextItem item);
	}
	
	private final FileSystem fs;
	private final Path formalContextPath;
	private final Path objectsIdsByAttrybuteBasePath;
	
	
	private FormalContextItemBuilder itemBuilder = new FormalContextItemBuilder();
	
	public FormalContextItemRepository(final FileSystem fs, final Path formalContextPath, final Path objectsIdsByAttributeBasePath){
		this.fs = fs;
		this.formalContextPath = formalContextPath;
		this.objectsIdsByAttrybuteBasePath = objectsIdsByAttributeBasePath;
	}
	
	public FormalContextItem find(final String objectId) throws IOException{
		
		FormalContextItem foundItem = null;
		
		final List<String> ids = Arrays.asList(new String[]{objectId});				
		
		final List<FormalContextItem> items = find(ids);
		
		if (items.size() > 0){
			foundItem = items.get(0);
		}
		
		return foundItem;
	}
	
	public List<FormalContextItem> find(final Collection<String> objectIds) throws IOException{
		
		final List<FormalContextItem> foundItems = new ArrayList<>();
							
		final FormalContextItemProcessor findObjectsProcessor = new FormalContextItemProcessor() {

			@Override
			public boolean process(final FormalContextItem nextItem) {
				
				boolean allObjectsWereFound = false;
				
				if (objectIds.contains(nextItem.getObjectId())){
					
					foundItems.add(nextItem);									
					allObjectsWereFound = foundItems.size() >= objectIds.size();
				}								
				
				return allObjectsWereFound;
			}			
		};
		
		processFormalContextFile(findObjectsProcessor);	
		
		return foundItems;
	}
	
	public List<String> findAllObjectIds() throws IOException {
		
		final TreeSet<String> objectsIds = new TreeSet<String>();
		
		final FormalContextItemProcessor allObjectsProcessor = new FormalContextItemProcessor() {

			@Override
			public boolean process(final FormalContextItem item) {
				
				objectsIds.add(item.getObjectId());
				
				return false;
			}			
		};
		
		processFormalContextFile(allObjectsProcessor);				
				
		return new ArrayList<>(objectsIds);
	}
	
	public List<String> findObjectIdsByAttribute(final String attb) throws IOException {
		
		final TreeSet<String> objectsIds = new TreeSet<String>();
										
		for (final FileStatus currentFileStatus : filesWithIdsForAttb(attb)){
			
			final List<String> loadIdsFromCurrentPath = loadObjectIdsfromPath(currentFileStatus.getPath());
			
			objectsIds.addAll(loadIdsFromCurrentPath);
		}				
		
		return new ArrayList<>(objectsIds);
	}
	
	public List<String> findAllAttbs() throws IOException {
		
		final List<String> attbs = new ArrayList<String>();
		
		final FormalContextItemProcessor allAttbsProcessor = new FormalContextItemProcessor() {

			@Override
			public boolean process(final FormalContextItem item) {
				
				for (String currentAttbName : item.allAttbNamesInOrder()){
					attbs.add(currentAttbName);
				}
				
				return true;
			}			
		};
		
		processFormalContextFile(allAttbsProcessor);				
		
		return attbs;
	}
	
	public List<FormalContextItem> findByAttb (final String attb) throws IOException{
	
		final List<FormalContextItem> foundItems = new ArrayList<>();
		
		final FormalContextItemProcessor findObjectsByAttbProcessor = new FormalContextItemProcessor() {

			@Override
			public boolean process(final FormalContextItem nextItem) {
												
				if (nextItem.hasAttb(attb)){
					foundItems.add(nextItem);
				}
				
				return false;
			}			
		};
		
		processFormalContextFile(findObjectsByAttbProcessor);	
		
		return foundItems;
	}	

	private FileStatus[] filesWithIdsForAttb(final String attb) throws FileNotFoundException, IOException {
		return fs.listStatus(this.objectsIdsByAttrybuteBasePath, buildObjectIdsByAttbFilter(attb));
	}

	private GlobFilter buildObjectIdsByAttbFilter(final String attb) throws IOException {
		return new GlobFilter(attb.concat("*"));
	}
	
	private List<String> loadObjectIdsfromPath (final Path file) throws IOException{
		
		final List<String> objectsIds = new ArrayList<String>();
		
		final Configuration conf = fs.getConf();
		final Option filePath = SequenceFile.Reader.file(file);
		
		try (SequenceFile.Reader objectIdReader = new SequenceFile.Reader(conf, filePath)){
			
			final Writable key = (Writable) ReflectionUtils.newInstance(objectIdReader.getKeyClass(), conf);
	        
	        final Writable value = (Writable) ReflectionUtils.newInstance(objectIdReader.getValueClass(), conf);
	        	        
	        while (objectIdReader.next(key, value)){
	        	objectsIds.add(value.toString());
	        }			
		}		
		
		return objectsIds;		
	}	
	
	private void processFormalContextFile(final FormalContextItemProcessor processor) throws IOException {
						
		final Configuration conf = fs.getConf();
		final Option filePath = SequenceFile.Reader.file(formalContextPath);
        
		try (SequenceFile.Reader formalContextReader = new SequenceFile.Reader(conf, filePath)){
			
			final Writable key = (Writable) ReflectionUtils.newInstance(formalContextReader.getKeyClass(), conf);
	        
	        final Writable value = (Writable) ReflectionUtils.newInstance(formalContextReader.getValueClass(), conf);
	        
	        boolean found = false;
	        while (!found && formalContextReader.next(key, value)){
	        	
	        	final String nextObjectId =  key.toString();
	        	final SortedMapWritable rawAttbs = (SortedMapWritable) value;
	        	
	        	final FormalContextItem nextItem = itemBuilder.build(nextObjectId, rawAttbs);
	        	
	        	found = processor.process(nextItem);
	        }			
		}
	}

}
