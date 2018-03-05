package com.mgarciaroig.fca.web.persistence.data.model;

import java.util.Collection;
import java.util.TreeSet;

public class Coincidence {
	
	private static final String CLUSTER_FLAG = "-CLUSTER";

	private static final String ENUM_FLAG = "-ENUM";

	private final TreeSet<String> objects = new TreeSet<>();
	
	private final TreeSet<String> fields = new TreeSet<>();
	
	public TreeSet<String> getObjects() {
		return objects;
	}

	public TreeSet<String> getFields() {
		return fields;
	}
	
	public int getNumberOfFields(){
		return fields.size();
	}
	
	public int getNumberOfObjects(){
		return objects.size();
	}
	
	void addObjects(final Collection<String> objects){
		
		for (final String currentObject : objects){
			this.objects.add(currentObject);
		}
	}
	
	void addAttributes(final Collection<String> attbs){
		
		for (final String currentAttb : attbs){
			addFieldFromAttb(currentAttb);		
		}				
	}

	private void addFieldFromAttb(final String attb) {
		
		String fieldToAdd = null;
		
		if (attb.contains(ENUM_FLAG)){
			fieldToAdd = fieldFromEnumAttb(attb);
		}
		else if (attb.contains(CLUSTER_FLAG)){
			fieldToAdd = fieldFromClusterAttb(attb);
		}
		else {
			fieldToAdd = fieldFromBooleanAttb(attb);
		}
		
		this.fields.add(fieldToAdd);				
	}	

	private String fieldFromEnumAttb(final String attb) {
		
		return extractField(attb, ENUM_FLAG);				
	}		
	
	private String fieldFromClusterAttb(final String attb) {
		
		return extractField(attb, CLUSTER_FLAG);				
	}
	
	private String fieldFromBooleanAttb(String attb) {
		
		String field = null;
		
		String booleanTrueSuffix = "-TRUE";
		String booleanFalseSuffix = "-FALSE";
		String booleanUndefinedSuffix = "-UND";
		
		if (attb.endsWith(booleanTrueSuffix)){
			field = extractField(attb, booleanTrueSuffix);
		}
		else if (attb.endsWith(booleanFalseSuffix)){
			field = extractField(attb, booleanFalseSuffix);
		}
		else if (attb.endsWith(booleanUndefinedSuffix)){
			field = extractField(attb, booleanUndefinedSuffix);
		}
		
		return field;
	}
	
	private String extractField(final String attb, final String flag){
		
		String field = null;
		
		int index = attb.indexOf(flag);
		
		if (index > -1){
			field = attb.substring(0, index);
		}
		
		return field;		
	}
}