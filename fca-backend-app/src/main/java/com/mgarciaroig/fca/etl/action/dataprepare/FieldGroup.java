package com.mgarciaroig.fca.etl.action.dataprepare;

public enum FieldGroup {
	
	LOCATION("Location"),
	FACILITY("Facility"),
	STATUS("Status"),	
	CATEGORY("Category"),
	INFORMATION("Information"),
	TECHDATA("TechData"),
	EXPERIMENTAL("Experimental"),
	UTILIZATION("Utilization");
	
	private final String msg;
	
	private FieldGroup (final String msg){
		this.msg = msg;
	}
	
	@Override
	public String toString(){
		return msg;
	}		
}
