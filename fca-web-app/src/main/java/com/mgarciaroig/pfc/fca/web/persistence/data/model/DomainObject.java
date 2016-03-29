package com.mgarciaroig.pfc.fca.web.persistence.data.model;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.mgarciaroig.pfc.fca.web.controller.AppController;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class DomainObject {
	
	private static Set<String> dateFields;
	
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(AppController.class);
	
	static {
		try {
			
			dateFields = dateFieldsConfiguration();
		
		} catch (final IOException error) {
			
			logger.error("Error retrieving date fields info", error);
			
			Throwables.propagate(error);
		}
	}

	private final String id;
	
	private final ObjectContent content = new ObjectContent();
	
	private final ObjectSimilitudes similitudes = new ObjectSimilitudes();
	
	private final DateFormat df = new SimpleDateFormat("yyy-MM-dd");
			
	DomainObject(final String id) throws IOException{
		this.id = id;		
	}

	public String getId() {
		return id;
	}

	public ObjectContent getContent() {
		return content;
	}	
	
	public ObjectSimilitudes getSimilitudes(){
		return this.similitudes;
	}
	
	void addField(final String fieldName, final String fieldValue){
		content.addField(fieldName, fieldValueFor(fieldName, fieldValue));
	}	
	
	void addSimilitude(final Collection<String> objects, final Collection<String> attbs){
		
		this.similitudes.addCoincidence(objects, attbs); 
	}
	
	private String fieldValueFor(final String fieldName, final String rawFieldValue){
		
		String fieldValue = rawFieldValue;
		
		if (dateFields.contains(fieldName)){
			fieldValue = parseDateFieldValue(rawFieldValue);
		}		
		
		return fieldValue;
	}
	
	private String parseDateFieldValue(String rawFieldValue) {
		
		String parsed = "";
		
		if (rawFieldValue != null && !rawFieldValue.trim().isEmpty()){
			parsed = df.format(new Date(Long.valueOf(rawFieldValue)));
		}
		
		return parsed;
	}

	private static Set<String> dateFieldsConfiguration() throws IOException{
		
		final Set<String> dateFields = new HashSet<>();
		
		final Properties dateFieldsProps = loadDateFieldsProperties();
		
		for (final String currentFieldName : dateFieldNames(dateFieldsProps)){
			dateFields.add(currentFieldName);
		}
		
		return dateFields;
	}

	private static String[] dateFieldNames(final Properties dateFieldsProps) {
		return dateFieldsProps.getProperty("date_fields", "").split(",");
	}
	
	private static Properties loadDateFieldsProperties() throws IOException{
		
		final Properties props = new Properties();
		
		props.load(DomainObject.class.getResourceAsStream("datefields.properties"));
		
		return props;
	}
}
