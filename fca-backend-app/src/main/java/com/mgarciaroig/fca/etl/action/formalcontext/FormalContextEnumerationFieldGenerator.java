package com.mgarciaroig.fca.etl.action.formalcontext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.mgarciaroig.fca.etl.action.discretization.codifier.FieldCodesRetriever;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import com.mgarciaroig.fca.etl.action.dataprepare.Field;

class FormalContextEnumerationFieldGenerator extends FormalContextFieldGeneratorBase {
	
	private FieldCodesRetriever codesFinder;
	
	FormalContextEnumerationFieldGenerator(final FieldCodesRetriever codesFinder){
		this.codesFinder = codesFinder;
	}

	@Override
	TreeMap<Text, BooleanWritable> generateFormalContextFields(final Field field, final Writable fieldValue) throws IOException {
		
		final TreeMap<Text, BooleanWritable> formalContextFields = new TreeMap<>(); 
		
		final Map<String, Integer> fieldCodesMapping = codesFinder.codesFor(field);
							
		final List<Long> fieldValues = parseFieldValues(fieldValue);
		
		for (final Entry<String, Integer> currentFieldCodeMapping : fieldCodesMapping.entrySet()){
					
						
			final Long currentCode = new Long(currentFieldCodeMapping.getValue());
			
			final Text formalContextFieldName = buildFormalContextFieldName(field, currentCode);
			
			final BooleanWritable formalContextFieldValue = new BooleanWritable(fieldValues.contains(currentCode));
			
			formalContextFields.put(formalContextFieldName, formalContextFieldValue);			
		}				
		
		return formalContextFields;
	}

	private List<Long> parseFieldValues(final Writable fieldValue) {
		
		final List<Long> fieldValues = new ArrayList<Long>();
		
		if (isEmpty(fieldValue)){
			addEmptyFieldValue(fieldValues);
		}
		else {
								
			for (final String currentRawFieldValue : StringUtils.split(fieldValue.toString(), ' ')){
			
				fieldValues.add(Long.valueOf(currentRawFieldValue));
			}
		}
		
		return fieldValues;
	}

	private void addEmptyFieldValue(final List<Long> fieldValues) {
		
		final long emptyFieldValueCode = 0L;
		
		fieldValues.add(emptyFieldValueCode);
	}

	private boolean isEmpty(final Writable fieldValue) {
		return (fieldValue == NullWritable.get()) || (fieldValue.toString().trim().isEmpty());
	}		

	private Text buildFormalContextFieldName(final Field field, final Long currentFieldCode) {
		
		return buildFormalContextFieldName(field, "ENUM-".concat(format(currentFieldCode)));
	}		

}
