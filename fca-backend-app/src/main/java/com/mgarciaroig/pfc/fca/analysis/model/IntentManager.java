package com.mgarciaroig.pfc.fca.analysis.model;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

class IntentManager {

	protected Set<String> attributesUpTo(final Collection<String> intent, final String toAttribute){
		
		final Set<String> attbs = new TreeSet<>();
		
		for (final String currentAttb : intent){
			
			if (currentAttb.equals(toAttribute)) break;
				
			attbs.add(currentAttb);
		}
		
		return attbs;
	}
	
}
