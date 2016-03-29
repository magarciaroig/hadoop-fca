package com.mgarciaroig.pfc.fca.web.persistence.data.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

public class ObjectSimilitudes {
	
	private final TreeMap<Integer,CoincidenceGroup> coincidences = new TreeMap<>();
	
	public List<CoincidenceGroup> getCoincidencesByRelevance(){
		
		final List<CoincidenceGroup> coincidencesByRelevance = new ArrayList<>();
		
		for (final int currentNumberOfAttbs : coincidences.descendingKeySet()){
			coincidencesByRelevance.add(this.coincidences.get(currentNumberOfAttbs));
		}
		
		return coincidencesByRelevance;
	}	
	
	void addCoincidence(Collection<String> objects, Collection<String> attbs){
						
		final CoincidenceGroup coincidenceGroup = coincidencesByAttbNumber(attbs.size());
					
		coincidenceGroup.addCoincidence(objects, attbs);		
	}

	private CoincidenceGroup coincidencesByAttbNumber(final int attbsNumber) {
		
		CoincidenceGroup group = null;
		
		if (coincidences.containsKey(attbsNumber)){
			group = this.coincidences.get(attbsNumber);
		}
		else {
			group = new CoincidenceGroup(attbsNumber);
			this.coincidences.put(attbsNumber, group);
		}
		
		return group;
	}	
}
