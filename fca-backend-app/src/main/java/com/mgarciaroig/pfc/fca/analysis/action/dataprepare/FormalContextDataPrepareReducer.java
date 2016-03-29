package com.mgarciaroig.pfc.fca.analysis.action.dataprepare;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Reducer to collect all object id's by enabled attribute
 * @author Miguel √?ngel Garc√≠a Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalContextDataPrepareReducer extends
		Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;	
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
    }
	
	@Override
	public void reduce (final Text attbName, final Iterable<Text> objectIds, final Context context) throws IOException, InterruptedException {
		
		final String namedOutput = buildNamedOutput(attbName);
		
		final Map<String,String> persistedObjectIds = new HashMap<>();
		
		for (final Text currentObjectId : objectIds){
			
			final String id = currentObjectId.toString();
			
			if (!persistedObjectIds.containsKey(id)){
				
				mos.write(NullWritable.get(), currentObjectId, namedOutput);
				
				persistedObjectIds.put(id, null);
			}						
		}		
	}		
	
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {		
		mos.close();
    }
	
	private String buildNamedOutput(final Text attbName){
		return "./".concat(attbName.toString());
	}
}
