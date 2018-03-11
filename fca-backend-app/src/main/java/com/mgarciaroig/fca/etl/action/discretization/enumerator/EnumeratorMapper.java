package com.mgarciaroig.fca.etl.action.discretization.enumerator;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import com.mgarciaroig.fca.etl.action.dataprepare.Field;
import com.mgarciaroig.fca.etl.action.discretization.DiscretizationClassifier;
import com.mgarciaroig.fca.etl.action.discretization.DiscretizationType;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Hadoop mapper to enumerate distinct values for enumarable fields
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class EnumeratorMapper extends
		Mapper<NullWritable, SortedMapWritable, Text, Text> {		
	
	private final DiscretizationClassifier classifier;
	
	private final Text keyToEmit = new Text();
	private final Text valueToEmit = new Text();
	
	
	public EnumeratorMapper() throws IOException{
		
		classifier = new DiscretizationClassifier();		
	}
	
	@Override
	public void map (final NullWritable key, final SortedMapWritable value, final Context context) throws IOException, InterruptedException{					
	
		final Set<Entry<WritableComparable, Writable>> register = value.entrySet();
		
		for (final Entry<WritableComparable, Writable> currentFieldData : register){
									
			final String fieldName = currentFieldData.getKey().toString();			
			
			final Field currentField = Field.buildFieldFromStringfiedRepresentation(fieldName);
			final DiscretizationType discretizationToApply = classifier.classify(currentField);
			
			if (discretizationToApply.equals(DiscretizationType.ENUMERATION)){
				
				String currentValue = "";
				
				final Writable fieldValue = currentFieldData.getValue();
				if ( !(fieldValue instanceof NullWritable) ){
					currentValue = fieldValue.toString();
				}
				
				emitValuesToReducer(currentField, currentValue, context);
			}
		}
		
	}
	
	private void emitValuesToReducer(final Field field, final String value, final Context context) throws IOException, InterruptedException{
								
		final String[] valuesToEmit = splitValues(value);
		if (valuesToEmit != null){
			
			keyToEmit.set(field.toString());
			
			for (final String currentValueToEmit : valuesToEmit){
				
				valueToEmit.set(currentValueToEmit);
				context.write(keyToEmit, valueToEmit);
			}
		}								
	}
	
	private String[] splitValues(final String rawFieldValue){
		
		final String[] splitedValues = rawFieldValue.split(",");
		
		if (splitedValues != null){
		
			for (int valueIndex = 0; valueIndex < splitedValues.length; valueIndex++){
				splitedValues[valueIndex] = splitedValues[valueIndex].trim().toUpperCase(); 
			}
		}
		
		return splitedValues;		
	}
}

