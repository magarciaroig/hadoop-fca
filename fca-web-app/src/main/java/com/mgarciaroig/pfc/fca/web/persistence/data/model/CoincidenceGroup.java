package com.mgarciaroig.pfc.fca.web.persistence.data.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CoincidenceGroup {
	
	private final int attbsNumber;
	
	private final List<Coincidence> coincidences = new ArrayList<>();

	CoincidenceGroup(final int attbsNumber){
		this.attbsNumber = attbsNumber;
	}

	public int getAttbsNumber() {
		return attbsNumber;
	}

	public List<Coincidence> getCoincidences() {
		return coincidences;
	}
	
	void addCoincidence(final Collection<String> objects, final Collection<String> attbs){
		
		final Coincidence toBeAdded = new Coincidence();
		
		toBeAdded.addObjects(objects);
		toBeAdded.addAttributes(attbs);
		
		coincidences.add(toBeAdded);
	}	
}
