package com.mgarciaroig.fca.etl.action.formalcontext;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.mgarciaroig.pfc.fca.etl.action.discretization.DiscretizationType;
import com.mgarciaroig.pfc.fca.etl.action.discretization.codifier.FieldCodesRetriever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;
import com.mgarciaroig.pfc.fca.etl.action.discretization.codifier.CodifierFilter;

public class FormalContextMapper extends Mapper<NullWritable, SortedMapWritable, Text, SortedMapWritable> {
	
	private static final String KMEANS_OUTPUT_BASE_PATH_PROPERTY = "etlHdfsMahoutKmeansOutput";
	
	private static final String ENUMERATIONS_PATH_PROPERTY =  "etlHdfsEnumerationsDir";		

	private final Text id = new Text();
	
	private CodifierFilter discretizationFilter;
				
	private FormalContextBooleanFieldGenerator booleanFieldsGenerator = new FormalContextBooleanFieldGenerator();		
	
	private FormalContextKmeansFieldGenerator kmeansFieldsGenerator;
	
	private FormalContextEnumerationFieldGenerator emumerationFieldsGenerator;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
						
		initDiscretizationFilter(context);				
						
		initKmeansFieldsGenerator(context);
		
		initEnumerationFieldsGenerator(context);
	}		
	
	@Override
	public void map (final NullWritable key, final SortedMapWritable record, final Context context) throws IOException, InterruptedException {				
		
		final SortedMapWritable processedRecord = new SortedMapWritable();
		
		for (final Entry<WritableComparable, Writable> currentEntry : record.entrySet()){
			
			final WritableComparable fieldName = currentEntry.getKey();			
			final Writable fieldValue = currentEntry.getValue();
			
			final Field currentField = Field.buildFieldFromStringfiedRepresentation(fieldName.toString());			
			
			if (currentField == null){
				setOutputKey(fieldValue);
			}
			else {
				processAllInputFields(processedRecord, currentField, fieldValue);
			}
		}
		
		context.write(id, processedRecord);
	}

	private void setOutputKey(final Writable fieldValue) {
		
		id.set(fieldValue.toString());
	}

	private void processAllInputFields(final SortedMapWritable processedRecord,
			final Field currentField, 
			final Writable fieldValue) throws IOException 
	{
		
		final TreeMap<Text,BooleanWritable> formalContextFields = processInputField(currentField, fieldValue);
		
		for (final Entry<Text, BooleanWritable> currentFormalContextFieldEntry: formalContextFields.entrySet()){
			
			final Text formalContextField = currentFormalContextFieldEntry.getKey();
			final BooleanWritable formalContextValue = currentFormalContextFieldEntry.getValue();
			
			processedRecord.put(formalContextField, formalContextValue);
		}				
	}


	private TreeMap<Text,BooleanWritable> processInputField(final Field field, final Writable fieldValue) throws IOException {
								
		final String fieldName = field.toString();
		
		TreeMap<Text,BooleanWritable> processedRecord = new TreeMap<Text,BooleanWritable>();
		
		if (discretizationFilter.isWanted(fieldName)){
		
			if (discretizationFilter.isDiscretizableAsBoolean(fieldName)){
				
				processedRecord = booleanFieldsGenerator.generateFormalContextFields(field, fieldValue);
			}
			
			else if (discretizationFilter.isDiscretizableAsEnumeration(fieldName)){
				
				processedRecord = emumerationFieldsGenerator.generateFormalContextFields(field, fieldValue);
				
			}
			else if (discretizationFilter.isDiscretizableAsKmeans(fieldName)){
				
				processedRecord = kmeansFieldsGenerator.generateFormalContextFields(field, fieldValue);
			}
		}
		
		return processedRecord;		
	}
	
	private FileSystem getFilesystem(Context context) throws IOException {
		return FileSystem.get(getConfiguration(context));
	}

	private Configuration getConfiguration(Context context) {
		return context.getConfiguration();
	}
	
	private void initDiscretizationFilter(Context context) throws IOException {
		
		final DiscretizationType[] allDiscretizationTypes = new DiscretizationType[]{DiscretizationType.ENUMERATION, DiscretizationType.BOOLEAN, DiscretizationType.KMEANS};
		discretizationFilter = new CodifierFilter(getConfiguration(context), allDiscretizationTypes);
	}

	private void initKmeansFieldsGenerator(Context context) throws IOException {
		
		final String clusterDataBasePathName = getConfiguration(context).getTrimmed(KMEANS_OUTPUT_BASE_PATH_PROPERTY);
		kmeansFieldsGenerator = new FormalContextKmeansFieldGenerator(getFilesystem(context), clusterDataBasePathName);
	}

	private void initEnumerationFieldsGenerator(Context context) throws IOException {
		
		final String enumerationsPathName = getConfiguration(context).getTrimmed(ENUMERATIONS_PATH_PROPERTY);
		
		final FieldCodesRetriever fieldCodesFinder = new FieldCodesRetriever(getFilesystem(context), new Path(enumerationsPathName));
		
		emumerationFieldsGenerator = new FormalContextEnumerationFieldGenerator(fieldCodesFinder);
	}		
}