package com.mgarciaroig.pfc.fca.web.persistence.data.model;

import org.apache.commons.lang.builder.CompareToBuilder;

public class Field implements Comparable<Field>{
	
	private final String name;
	
	private final String value;
	
	Field(final String name, final String value){
		
		this.name = name;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public String getValue() {
		return value;
	}

	@Override
	public int compareTo(final Field other) {
		
		final CompareToBuilder cb = new CompareToBuilder();
		
		cb.append(name, other.name);
		
		return cb.toComparison();
	}
}
