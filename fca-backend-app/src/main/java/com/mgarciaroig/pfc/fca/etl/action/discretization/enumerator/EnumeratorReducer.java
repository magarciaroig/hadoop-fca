package com.mgarciaroig.pfc.fca.etl.action.discretization.enumerator;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**
 * Hadoop reduce to enumerate distinct values for enumarable fields
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class EnumeratorReducer extends
		Reducer<Text, Text, IntWritable, Text> {
	
	private IntWritable key = new IntWritable();
	private Text value = new Text();
	
	private MultipleOutputs<IntWritable, Text> mos;
	
	private Logger logger = Logger.getLogger(EnumeratorReducer.class);
	
	private final HashMap<String,String> processedValues = new HashMap<String,String>();
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<IntWritable, Text>(context);
    }
	
	@Override
	public void reduce(final Text fieldName, final Iterable<Text> fieldValues, final Context context) throws IOException, InterruptedException{
		
		final String stringfiedFieldName = fieldName.toString();
		final String namedOutput = buildNamedOutput(stringfiedFieldName);
		
		logger.info("Reducing " + stringfiedFieldName + " to output " + namedOutput);
				
		
		try {
		
			int currentKeyValue = 0;
			
			currentKeyValue = persistIfValueIsNew(fieldName.toString(), currentKeyValue, "");						
			
			
			for (final Text currentFieldValue : fieldValues) {
				
				final String stringfiedFieldValue = currentFieldValue.toString().trim();
				
				currentKeyValue = persistIfValueIsNew(fieldName.toString(), currentKeyValue, stringfiedFieldValue);								
			}
		}
		finally {
			processedValues.clear();
		}
	}	
		
	private int persistIfValueIsNew(final String fieldName, final int code, final String value) throws IOException, InterruptedException{
		
		int currentKeyValue = code;
		 
		if (!processedValues.containsKey(value)){			
			
			persistEnumValue(fieldName, code, value);
			processedValues.put(value, value);
			currentKeyValue++;
		}
		
		return currentKeyValue;
		
	}
	
	private void persistEnumValue(final String fieldName, final int code, final String value) throws IOException, InterruptedException{
		this.key.set(code);
		this.value.set(value);
		
		if (logger.isDebugEnabled()){
			logger.debug(String.format("Assigned field '%s' value '%s' code %d", fieldName, value, code));
		}
												
		mos.write(this.key, this.value, buildNamedOutput(fieldName));				
	}
	
	
	
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {		
		mos.close();
    }
	
	private String buildNamedOutput(final String stringfiedFieldName){
		return "./".concat(stringfiedFieldName);
	}
}
