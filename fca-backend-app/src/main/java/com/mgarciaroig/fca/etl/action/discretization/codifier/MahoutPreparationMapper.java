package com.mgarciaroig.fca.etl.action.discretization.codifier;

import java.io.IOException;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;
import com.mgarciaroig.pfc.fca.etl.action.discretization.DiscretizationType;

/**
 * Mapper to emit all mahout clusterizable values. with the name of the field as a key
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class MahoutPreparationMapper extends Mapper<NullWritable,SortedMapWritable,Text,DoubleWritable>{
	
	private static Logger logger = Logger.getLogger(MahoutPreparationMapper.class);

	private CodifierFilter codifierFilter;
	
	private Text fieldName = new Text("");
	private DoubleWritable valueToEmit = new DoubleWritable();
				
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		codifierFilter = new CodifierFilter(context.getConfiguration(), new DiscretizationType[]{DiscretizationType.KMEANS});
	}
	
	/**
	 * For each record, emit the mahout clusterizable values; with the name of the field as a key
	 */
	@Override
	public void map (final NullWritable key, final SortedMapWritable record, final Context context) throws IOException, InterruptedException {		
		
		final Set<Entry<WritableComparable, Writable>> entries = record.entrySet();
		
		for (final Entry<WritableComparable, Writable> currentEntry : entries){
			
			log(String.format("Processing '%s':'%s'", currentEntry.getKey().toString(), currentEntry.getValue().toString()));
			
			final Field currentField = Field.buildFieldFromStringfiedRepresentation(currentEntry.getKey().toString());
			
			if (currentField != null){
							
				processField(context, currentField, currentEntry.getValue());
			}
		}				
	}

	private void processField(final Context context,			
			final Field field, final Writable fieldValue) throws IOException, InterruptedException {
		
		final Double processedValue = processInputRecord(field, fieldValue);
		
		if (processedValue != null){
			
			fieldName.set(field.toString());
			valueToEmit.set(processedValue);
			
			context.write(fieldName, valueToEmit);				
		}
	}
	
	private Double processInputRecord(final Field field, final Writable fieldValue) throws IOException  {
		
		if (isNotConvertibleToKMeansValue(field, fieldValue)){
			return null;
		}
		
		return Double.valueOf(fieldValue.toString());				
	}

	private boolean isNotConvertibleToKMeansValue(final Field field, final Writable fieldValue) {
		return isNotDiscretizableAsKMeans(field) || isANullFieldValue(fieldValue) || isNonNumericValue(fieldValue);
	}

	private boolean isNonNumericValue(final Writable fieldValue) {
		return fieldValue.toString().equals("");
	}

	private boolean isANullFieldValue(final Writable fieldValue) {
		return fieldValue.getClass().equals(NullWritable.class);
	}

	private boolean isNotDiscretizableAsKMeans(final Field field) {
		return !codifierFilter.isDiscretizableAsKmeans(field.toString());
	}	
	
	private void log(final String message){
		
		if (logger.isDebugEnabled()){
			logger.debug(message);
		}
	}
}
