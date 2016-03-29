package com.mgarciaroig.pfc.fca.etl.action.formalcontext;

import java.util.TreeMap;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;

public class FormalContextBooleanFieldGenerator extends FormalContextFieldGeneratorBase {
	
	private final static String expctedValueForNotDefined = "0";
	private final static String expctedValueForTrue = "1";
	private final static String expctedValueForFalse = "2";		

	@Override
	TreeMap<Text, BooleanWritable> generateFormalContextFields(final Field field, final Writable fieldValue) {
		
		final Text fieldNameForNotDefinedValue =  fieldNameForNotDefinedValue(field);
		final Text fieldNameForTrueValue =  fieldNameForTrueValue(field);
		final Text fieldNameForFalseValue =  fieldNameForFalseValue(field);
		
		final BooleanWritable fieldValueForNotDefined = valueForField(fieldValue, expctedValueForNotDefined);
		final BooleanWritable fieldValueForTrue = valueForField(fieldValue, expctedValueForTrue);
		final BooleanWritable fieldValueForFalse = valueForField(fieldValue, expctedValueForFalse);
		
		final TreeMap<Text, BooleanWritable> formalContextFields = new TreeMap<>();		
		
		formalContextFields.put(fieldNameForNotDefinedValue, fieldValueForNotDefined);		
		formalContextFields.put(fieldNameForTrueValue, fieldValueForTrue);		
		formalContextFields.put(fieldNameForFalseValue, fieldValueForFalse);
		
		return formalContextFields;
	}		
	
	private Text fieldNameForFalseValue(final Field field) {
		return buildFormalContextFieldName(field, "FALSE");
	}		

	private Text fieldNameForTrueValue(final Field field) {
		return buildFormalContextFieldName(field, "TRUE");
	}	

	private Text fieldNameForNotDefinedValue(final Field field) {
		return buildFormalContextFieldName(field, "UND");
	}
}