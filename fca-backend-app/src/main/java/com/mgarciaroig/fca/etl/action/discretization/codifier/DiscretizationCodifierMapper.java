package com.mgarciaroig.fca.etl.action.discretization.codifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Mapper to codify the values for enumerable fields
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class DiscretizationCodifierMapper extends Mapper<NullWritable, SortedMapWritable, NullWritable, SortedMapWritable>{

	private static final String ETL_HDFS_ENUMERATIONS_DIR = "etlHdfsEnumerationsDir";

	private FieldCodesRetriever fieldCodesRetriever;
	
	private Map<String,Map<String,Integer>> codesCache;
		
	
	private CodifierFilter codifierFilter;
	
	private final static Logger logger = Logger.getLogger(DiscretizationCodifierMapper.class);		
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		
		if (fieldCodesRetriever == null){
		
			final Configuration config = context.getConfiguration();
		
			final FileSystem fs = FileSystem.get(config);
			final String baseEnumerationFilesPathName = config.get(ETL_HDFS_ENUMERATIONS_DIR);
		
			fieldCodesRetriever = new FieldCodesRetriever(fs, new Path(baseEnumerationFilesPathName));				
		
			codesCache = new HashMap<String,Map<String,Integer>>();
			
			codifierFilter = new CodifierFilter(context.getConfiguration(), new DiscretizationType[]{DiscretizationType.ENUMERATION, DiscretizationType.BOOLEAN});
		}
    }
	
	@Override
	public void map (final NullWritable key, final SortedMapWritable record, final Context context) throws IOException, InterruptedException{
				
		final SortedMapWritable currentOutput = new SortedMapWritable();
		
		final Set<Entry<WritableComparable, Writable>> entries = record.entrySet();
		
		for (final Entry<WritableComparable, Writable> currentEntry : entries){
			
			final Writable processedValue = processInputRecord(currentEntry);
			currentOutput.put(currentEntry.getKey(), processedValue);
		}
		
		context.write(NullWritable.get(), currentOutput);			
	}

	private Writable processInputRecord(final Entry<WritableComparable, Writable> fieldAndValue) throws IOException  {
		
		Writable value = fieldAndValue.getValue();
											
		final String stringfiedField = fieldAndValue.getKey().toString();			
											
		log(String.format("BEGIN Found field '%s'", stringfiedField));	
		
		if (codifierFilter.isWanted(stringfiedField)){
			
			final Field currentField = Field.buildFieldFromStringfiedRepresentation(stringfiedField);
		
			if (codifierFilter.isDiscretizableAsEnumeration(stringfiedField)){
														
				value = parseEnumerableValues(currentField, fieldAndValue.getValue());
																					
			}
			else if (codifierFilter.isDiscretizableAsBoolean(stringfiedField)){
				
				value = parseBooleanValue(fieldAndValue.getValue());																								 			
			}
			else {
				log(String.format("Non enumerable field '%s' found", stringfiedField));				
			}			
									
			log(String.format("END Found field '%s'", stringfiedField));												
		}
		
		
		return value;				
	}		
	
	private Text parseBooleanValue(final Writable rawValue){
		
		final Boolean booleanFieldValue = convertToBoolean(rawValue);
		return booleanValueToText(booleanFieldValue);				
	}
	
		
	private Text booleanValueToText(final Boolean value){
		String stringfiedValue = "0";
		
		if (value != null){
			stringfiedValue = value ? "1" : "2";
		}
		
		return new Text(stringfiedValue);
	}
	
	private Boolean convertToBoolean(final Writable rawValue){
		
		final String StringfiedValue = rawValue.toString().trim().toLowerCase();
		
		if (StringfiedValue.equals("")){
			return null;
		}
		
		return StringfiedValue.startsWith("t") || StringfiedValue.startsWith("y") || StringfiedValue.startsWith("1");				
	}		
	
	private Writable parseEnumerableValues(final Field field, final Writable rawEnumerationValue) throws IOException{
					
		final String[] rawEnumerationValues = rawEnumerationValue.toString().split(",");
		final List<Text> enumerationValues = new ArrayList<Text>();
		
		for (final String currentRawEnumerationValue: rawEnumerationValues){
			
			final Text currentEnumerationValue = codify(field, currentRawEnumerationValue);
			
			if (currentEnumerationValue != null) enumerationValues.add(currentEnumerationValue);
		}
							
		final Text[] enumerationValuesArray =  enumerationValues.toArray(new Text[]{});
						
		return new Text(StringUtils.join(enumerationValuesArray, ' '));
	}
			
	private Text codify(final Field field, final String value) throws IOException{
						
		log(String.format("codify field '%s' ('%s') ", field.toString(), value));						
		
		retrieveCodeMapForField(field);
		
		final Map<String,Integer> fieldCodes = retrieveCodeMapForField(field);
		
		if (fieldCodes == null){
			throw new IOException(String.format("Unable to retireve field codes map for field %s", field.toString()));
		}
		 
		Integer code = fieldCodes.get(value.trim().toUpperCase());
		
		if (code == null){
			
			final String warnMsg = String.format("No code found for enumarion field '%s' ('%s')", field.toString(), value);
			
			logger.warn(warnMsg);
						
									
			for (final Entry<String, Integer> currentFieldCode: fieldCodes.entrySet()){
				log(String.format("Found value code mapping: '%s' => %d", currentFieldCode.getKey(), currentFieldCode.getValue()));
			}
						
			return null;
		}
		
		log(String.format("Codified field '%s' value '%s' as '%d'", field.toString(), value, code));		
		
		return new Text(code.toString());
	}
	
	private Map<String,Integer> retrieveCodeMapForField(final Field field) throws IOException{
		
		final String fieldName = field.toString();
		
		if (!codesCache.containsKey(fieldName)){
			
			final Map<String,Integer> codes = fieldCodesRetriever.codesFor(field);
			codesCache.put(fieldName, codes);
		}
		
		return codesCache.get(fieldName);
		
	}	
	
	private void log(final String msg){
		if (logger.isInfoEnabled()){
			logger.info(msg);		
		}
	}
}
