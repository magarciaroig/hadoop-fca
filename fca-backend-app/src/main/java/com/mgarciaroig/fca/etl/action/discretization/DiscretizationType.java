package com.mgarciaroig.fca.etl.action.discretization;

public enum DiscretizationType {

	NONE, ENUMERATION, KMEANS, BOOLEAN;
	
	public static DiscretizationType buildFromCode(final String code){
		return valueOf(code.trim().toUpperCase());
	}		
}
