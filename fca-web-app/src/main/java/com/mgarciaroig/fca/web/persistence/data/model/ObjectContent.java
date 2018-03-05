package com.mgarciaroig.fca.web.persistence.data.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ObjectContent {
	
	private final Map<String,Section> sections = new TreeMap<>();
	
	public List<Section> getSections(){
		
		final List<Section> sections = new ArrayList<>();
		
		for (final Section currentSection : this.sections.values()){
			sections.add(currentSection);
		}
		
		Collections.sort(sections);
		
		return sections;
	}
	
	void addField(final String fieldName, final String fieldValue){
		
		if (!fieldName.equalsIgnoreCase("id") && fieldValue != null && !fieldValue.trim().isEmpty()){
			
			final Section section = sectionForField(fieldName);				
			section.addField(shortenFieldName(section,fieldName), fieldValue);
		}
	}		
	
	private String shortenFieldName(final Section section, String fieldName) {
				
		return fieldName.substring(section.getName().length() + 1);		
	}

	private Section sectionForField(final String fieldName){
		
		Section section = null;
		
		final String sectionName = fieldSection(fieldName);	
		
		if (existSection(sectionName)) {
			section = sections.get(sectionName);
		}
		else {
			section = addSection(sectionName);
		}
		
		return section;
	}
	
	private String fieldSection(final String fieldName){
		
		return fieldName.split("\\.")[0];		
	}
	
	private boolean existSection(final String sectionName){
		return sections.containsKey(sectionName);
	}
	
	private Section addSection(final String sectionName){
		
		final Section section = new Section(sectionName);
		sections.put(sectionName, section);
		
		return section;
	}
}
