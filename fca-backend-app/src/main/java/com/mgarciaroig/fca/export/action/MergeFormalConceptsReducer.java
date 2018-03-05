package com.mgarciaroig.fca.export.action;

import java.io.IOException;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer to merge all generated formal concept in a single sequence file
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class MergeFormalConceptsReducer extends Reducer<NullWritable, FormalConcept, NullWritable, FormalConcept> {
	
	@Override
	public void reduce(final NullWritable key, final Iterable<FormalConcept> concepts, final Context context) throws IOException, InterruptedException {
		
		for (final FormalConcept currentConcept : concepts){
			context.write(key, currentConcept);
		}
	}
}
