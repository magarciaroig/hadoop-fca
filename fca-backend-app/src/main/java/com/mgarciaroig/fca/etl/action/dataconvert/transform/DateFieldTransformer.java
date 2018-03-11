package com.mgarciaroig.fca.etl.action.dataconvert.transform;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.mgarciaroig.fca.etl.action.dataconvert.FieldType;

class DateFieldTransformer implements Transformer {
	
	private final Logger logger = Logger.getLogger(DateFieldTransformer.class);
	
	private final String isoDateFormat = "yyyy-MM-dd";
	
	private final DateFormat formatter;
	
	DateFieldTransformer(){
		formatter = new SimpleDateFormat(isoDateFormat);
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	@Override
	public String transform(FieldType fieldType, String toBeTransformed) {
				
		if (!transformGuard(fieldType, toBeTransformed)) return null;		
				
		String transformed = null;
				
		final Long timestamp = stringfiedDateToTimestamp(toBeTransformed);
		if (timestamp != null) {
			transformed = timestamp.toString();
		}		
		
		return transformed;
	}

	private Long stringfiedDateToTimestamp(String toBeTransformed) {
		
		Long timestamp = null;
		
		try {
			
			final Date date = formatter.parse(toBeTransformed);
			timestamp = new Long(date.getTime());
								
			
		} catch (ParseException e) {
			
			if (logger.isDebugEnabled()){
				logger.debug(String.format("Unable to parse date from string %s", toBeTransformed));
			}
			
			timestamp = null;
		}
		
		return timestamp;
	}
	
	private boolean transformGuard(FieldType fieldType, String toBeTransformed){
		
		final boolean dataTypeMacthes = fieldType == FieldType.DATE_FIELD;
		
		final boolean nonEmptyData = toBeTransformed != null && !toBeTransformed.trim().isEmpty();				
		
		return dataTypeMacthes && nonEmptyData;				
	}			
}
