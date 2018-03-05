package com.mgarciaroig.fca.analysis.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Class to represent each object to be analyzed in the formal context
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class FormalContextItem {
	
	private final String objectId;	

	final private Map<String,Boolean> attbs;
	
	public FormalContextItem(final String id, final TreeMap<String,Boolean> attbs){
		this.objectId = new String(id);
		this.attbs = new TreeMap<String,Boolean> (attbs);
	}
	
	public boolean hasAttb(final String attb){
		return attributeExists(attb) && attbValue(attb);
	}
	
	public List<String> allAttbNamesInOrder(){
		
		final List<String> attbNames = new ArrayList<>();
		
		for (final Entry<String, Boolean> currentAttbEntry : attbs.entrySet()){
			attbNames.add(currentAttbEntry.getKey());
		}
		
		Collections.sort(attbNames);
		
		return attbNames;
	}

	private boolean attbValue(final String attb) {
		return attbs.get(attb).booleanValue();
	}

	private boolean attributeExists(final String attb) {
		return attbs.containsKey(attb);
	}
	
	public String getObjectId() {
		return objectId;
	}		
	
	@Override
	public boolean equals(final Object other){
		
		if (other instanceof FormalContextItem){
									
			final FormalContextItem otherItem = (FormalContextItem) other; 
			return getObjectId().equals(otherItem.getObjectId());
		}
		
		return false;
	}
	
	@Override
	public int hashCode(){
		return getObjectId().hashCode();
	}	
	
	@Override
	public String toString(){
		
		StringBuilder stringfiedAttbs = new StringBuilder();
		
		for (Entry<String, Boolean> currentAttbEntry : attbs.entrySet()){
			
			final String stringfiedAttb = String.format(" '%s' => %s", currentAttbEntry.getKey(), currentAttbEntry.getValue().toString());
			stringfiedAttbs.append(stringfiedAttb);
		}		
		
		return String.format("[%s '%s' (%s)]", getClass().getSimpleName(), getObjectId(), stringfiedAttbs.toString());
	}

}
