package com.mgarciaroig.fca.analysis.action.dataprepare;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to collect all object id's by enabled attribute
 * @author Miguel �?ngel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalContextDataPrepareMapper extends
		Mapper<Text, SortedMapWritable, Text, Text> {
			
	@Override
	public void map (final Text id, final SortedMapWritable record, final Context context) throws IOException, InterruptedException {
		
		for (final Entry<WritableComparable, Writable> currentAttbData : record.entrySet()){
			
			final Text attbName = (Text) currentAttbData.getKey();
			final BooleanWritable attbValue = (BooleanWritable) currentAttbData.getValue();
			
			if (attbValue.get()){
												
				context.write(attbName, id);
			}			
		}
	}
}
