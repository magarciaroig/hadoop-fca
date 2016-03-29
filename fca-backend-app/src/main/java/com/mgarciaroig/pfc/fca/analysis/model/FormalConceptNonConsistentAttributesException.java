package com.mgarciaroig.pfc.fca.analysis.model;

import java.util.Collection;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Class that modelizes an error trying to construct a new formal context; but passing one or more attributes not shared by all of the objects
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalConceptNonConsistentAttributesException extends FCAAnalizerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final Collection<FormalContextItem> objects;
	
	private final Collection<String> sharedAttbs;

	public FormalConceptNonConsistentAttributesException(final Collection<FormalContextItem> objects, final Collection<String> sharedAttbs) {
		
		super("The fornmal context attributes are not shared by all objects of the formal concept");
		
		this.objects = objects;
		this.sharedAttbs = sharedAttbs;
	}

	public Collection<FormalContextItem> getObjects() {
		return objects;
	}

	public Collection<String> getSharedAttbs() {
		return sharedAttbs;
	}	
}
