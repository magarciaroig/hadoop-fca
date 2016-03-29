package com.mgarciaroig.pfc.fca.etl.action.discretization.codifier;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;

public class FieldCodesRetriever {
	
	private static Logger logger = Logger.getLogger(FieldCodesRetriever.class);

	private final FileSystem fileSystem;
	private final Path baseEnumerationFilesPath;
	
	private Map<String,Map<String,Integer>> codesCache = new HashMap<>();
	
	public FieldCodesRetriever(final FileSystem fileSystem, final Path baseEnumerationFilesPath){
		
		this.fileSystem = fileSystem;
		this.baseEnumerationFilesPath = baseEnumerationFilesPath;
	}
	
	public Map<String,Integer> codesFor(final Field field) throws IOException {
		
		if (fieldCodesAvailableInCache(field)){
			return fieldCodesFromCache(field);
		}
		
		final HashMap<String,Integer> codes = new HashMap<String,Integer>();
						
		SequenceFile.Reader codesReader = null;
		
		try {
			
			final Configuration conf = fileSystem.getConf();
			final Option filePath = SequenceFile.Reader.file(buildCodesPathToReadFrom(field));
	        
			codesReader = new SequenceFile.Reader(conf, filePath);

	        final Writable key = (Writable) ReflectionUtils.newInstance(codesReader.getKeyClass(), conf);
	        
	        final Writable value = (Writable) ReflectionUtils.newInstance(codesReader.getValueClass(), conf);
	        	        	        
	        Text occurrence;
	        IntWritable code;
	        
	        while (codesReader.next(key, value)) {
	        	
	        	debug(String.format("Found (%s,%s): '%s' : '%s'", key.getClass().getName(), value.getClass().getName(), key.toString(), value.toString()));
	        	
	        	code = (IntWritable) key;
	        	occurrence = (Text) value;
	        	
	        	codes.put(occurrence.toString(), code.get());
	        	
	        	debug(String.format("Saved '%s' : '%d'", occurrence.toString(), code.get()));
	        }
		
		} catch (IOException e) {
			
			e.printStackTrace();	
			throw e;
			
		}
		finally {
			
			debug("Finished enumeration codes file processing");
			
			if (codesReader != null){
				codesReader.close();
			}
		}
		
		updateFieldCodesCache(field, codes);
		
		return codes;
	}

	private void updateFieldCodesCache(final Field field, final HashMap<String, Integer> codes) {
		codesCache.put(field.toString(), codes);
	}
	
	private boolean fieldCodesAvailableInCache(final Field field){
		return codesCache.containsKey(field.toString());
	}
	
	private Map<String,Integer> fieldCodesFromCache(final Field field){
		return codesCache.get(field.toString());
	}
	
	private Path buildCodesPathToReadFrom(final Field field){
		
		final String fileSuffix = "-r-00000";
		
		final Path codesPath = new Path(baseEnumerationFilesPath, field.toString().concat(fileSuffix));
		
		debug(String.format("Reading enumeration codes from file '%s'", codesPath.toString()));
		
		return codesPath;
	}	
	
	private void debug(final String msg){
		
		if (logger.isInfoEnabled()){
			logger.info(msg);
		}
	}
}
