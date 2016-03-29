package com.mgarciaroig.pfc.fca.analysis.model;

import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Utility class to build a concept item from serialization types 
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalContextItemBuilder {

	public FormalContextItem build(final String objectId, final SortedMapWritable rawAttbs) {
				
		final TreeMap<String,Boolean> attbs = new TreeMap<>();
		
		for (final Entry<WritableComparable, Writable> currentAttbEntry : rawAttbs.entrySet()){
			
			final String name = currentAttbEntry.getKey().toString();			
									
			final BooleanWritable attbValue = (BooleanWritable)  currentAttbEntry.getValue();
				
			attbs.put(name, attbValue.get());			
		}
		
		return new FormalContextItem(objectId, attbs);
	}
}
