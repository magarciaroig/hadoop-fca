package com.mgarciaroig.fca.export.action;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.mgarciaroig.fca.analysis.model.FormalConcept;
import com.mgarciaroig.fca.analysis.model.FormalConceptBuildingKey;

/**
 * Mapper to merge all generated formal concept in a single sequence file
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class MergeFormalConceptsMapper extends Mapper<FormalConceptBuildingKey, FormalConcept, NullWritable, FormalConcept> {
	
	private final NullWritable keyToBeEmitted = NullWritable.get();
	
	@Override
	public void map (final FormalConceptBuildingKey key, final FormalConcept concept, final Context context) throws IOException, InterruptedException{
				
		context.write(keyToBeEmitted, concept);
	}

}
