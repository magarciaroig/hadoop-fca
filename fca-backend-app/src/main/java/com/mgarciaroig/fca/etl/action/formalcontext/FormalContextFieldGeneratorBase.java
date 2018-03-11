package com.mgarciaroig.fca.etl.action.formalcontext;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.TreeMap;

import com.mgarciaroig.fca.etl.action.dataprepare.Field;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

abstract class FormalContextFieldGeneratorBase {
	
	protected static BooleanWritable trueValue = new BooleanWritable(true);
	protected static BooleanWritable falseValue = new BooleanWritable(false);
	
	private NumberFormat numberFormatter =  NumberFormat.getInstance();
	
	abstract TreeMap<Text,BooleanWritable> generateFormalContextFields(final Field field, final Writable fieldValue) throws IOException;
	
	protected FormalContextFieldGeneratorBase(){
		initNumberFormatter();
	}
	
	protected BooleanWritable valueForField(final Writable fieldValue, final String expectedFieldValue){
		return fieldValue.toString().equals(expectedFieldValue) ? trueValue : falseValue;
	}			
	
	protected Text buildFormalContextFieldName(final Field field, final String prefix) {
		
		final StringBuilder builder = new StringBuilder(field.toString());
		
		if (prefix != null){
			appendSeparator(builder).append(prefix);
		}				
		
		return new Text(builder.toString());
	}
	
	private StringBuilder appendSeparator(final StringBuilder builder) {
		return builder.append("-");
	}
	
	protected String format(long code){
		return numberFormatter.format(code);
	}
	
	private void initNumberFormatter() {
		numberFormatter.setMinimumIntegerDigits(3);
		numberFormatter.setMaximumFractionDigits(0);
	}
}
