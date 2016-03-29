package com.mgarciaroig.pfc.fca.etl.action.dataconvert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;

/*
 * Utility Class to persist records to a HDFS sequence file
 */
public class ConversionSequenceFileWriter {
	
	private final SequenceFile.Writer writer;
	
	private final List<Field> fields;
	
	private final SortedMapWritable map = new SortedMapWritable();
	
	private final Logger logger = Logger.getLogger(ConversionSequenceFileWriter.class);
		
	public ConversionSequenceFileWriter(final SequenceFile.Writer writer, final List<Field> fields) {	
		this.writer = writer;
		this.fields = new ArrayList<Field>(fields);
	}

	public void persistNewRegister(final List<Object> values) throws IOException{
						
		populateMap(values);
		
		writer.append(NullWritable.get(), map);		
	}
	
	private void populateMap(final List<Object> values){
		
		map.clear();
		
		int recordNumber = 0;
		
		for (final Field currentField : this.fields){
			
			Object currentValue = null;
			if  (recordNumber < values.size()){
				currentValue = values.get(recordNumber);
			}
			
			logger.debug("Generating persistable type for field " + currentField.name());
								
			map.put(new Text(currentField.toString()), convertToPersistableType(currentField, currentValue));
			
			recordNumber++;
		}
		
		map.put(new Text("id"), new Text(UUID.randomUUID().toString()));
		
	}
	
	private Writable convertToPersistableType(final Field field, final Object value){
		
		final Writable persistable;
						
		if (value == null){
			logger.debug("NullWritable type");
			persistable = NullWritable.get();
		}
		else if (value instanceof Integer){
			logger.debug("Integer type");
			persistable = new IntWritable((Integer) value);			
		}
		else if (value instanceof Byte){
			logger.debug("Byte type");
			persistable = new ByteWritable((Byte) value);			
		}
		else if (value instanceof Long){
			logger.debug("Long type");
			persistable = new LongWritable((Byte) value);			
		}
		else if (value instanceof Float){
			logger.debug("Float type");
			persistable = new FloatWritable((Float) value);			
		}
		else if (value instanceof Double){
			logger.debug("Double type");
			persistable = new DoubleWritable((Double) value);			
		}
		else if (value instanceof Date){
			logger.debug("Date type");
			persistable = new LongWritable(((Date) value).getTime());			
		}
		else {
			persistable = convertToPersistableTypeFromString(field, value.toString());			
		}		
		
		return persistable;
	}
	
	private Writable convertToPersistableTypeFromString(final Field field, final String value){
		
		final Writable persistable;
		
		final FieldType type = field.getType();
		
		if (type.equals(FieldType.BOOLEAN_FIELD)){			
			persistable = booleanWritableFromString(value);
			
		}
		else if (type.equals(FieldType.DATE_FIELD)){			
			persistable = longWritableFromString(value);
			
		}
		else if (type.equals(FieldType.DOUBLE_FIELD)){
			
			persistable =  doubleWritableFromString(value);
			 
		} 
		else if (type.equals(FieldType.LONG_FIELD)){
			
			persistable = longWritableFromString(value);
		}
		else {			
			persistable = new Text(value);
		}
		
		return persistable;		
	}
	
	private BooleanWritable booleanWritableFromString(final String value){
		
		final boolean bolValue = value.trim().toUpperCase().equals("TRUE"); 
		
		logger.debug("Boolean type");
		return new BooleanWritable(bolValue);
	}
	
	private LongWritable longWritableFromString(final String value){
		
		logger.debug("Long type");
		return new LongWritable(Long.parseLong(value.trim()));
	}
	
	private DoubleWritable doubleWritableFromString(final String value){
		
		logger.debug("Double type");
		return new DoubleWritable(Double.valueOf(value.trim()));
	}	
}
