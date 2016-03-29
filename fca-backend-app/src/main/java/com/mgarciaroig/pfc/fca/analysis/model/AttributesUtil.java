package com.mgarciaroig.pfc.fca.analysis.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Class in charge of performing generic attributes operations
 * 
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class AttributesUtil {
	
	private final List<String> allAttributesInOrder;
	
	public AttributesUtil(final List<String> allAttributesInOrder){
		this.allAttributesInOrder = allAttributesInOrder;
	}
	
	public List<String> attributesFrom(final String fromInclusive){
		
		final int attbPosition = attributePosition(fromInclusive);
		if (attbPosition < 0) return new ArrayList<String>();
		
		return this.allAttributesInOrder.subList(attbPosition, allAttributesInOrder.size());	
	}	

	public String nextAttribute(final String currentAttribute) {
		
		final int attbPosition = attributePosition(currentAttribute);
		
		if (attbPosition < 0) return null;
						
		if (attbPosition >= this.allAttributesInOrder.size() - 1) return currentAttribute;			
		
		return this.allAttributesInOrder.get(attbPosition + 1);
	}
	
	private int attributePosition(final String fromInclusive) {
		return allAttributesInOrder.indexOf(fromInclusive);
	}
}
