package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import com.mgarciaroig.fca.analysis.model.AttributesUtil;
import com.mgarciaroig.fca.analysis.model.FormalConcept;
import com.mgarciaroig.fca.analysis.model.FormalConceptBuildingKey;

public class FormalConceptBuildingReducer extends Reducer<FormalConceptBuildingKey,FormalConcept,FormalConceptBuildingKey,FormalConcept> {
	
	protected AttributesUtil attributesUtil;
	
	@Override
	public void setup(final Context context) throws IOException{
		
		final Configuration conf = context.getConfiguration();
		
		final FileSystem fs = FileSystem.get(conf);
		
		final RepositoryBuilder builder = new RepositoryBuilder(conf, fs);
		
		this.attributesUtil = new AttributesUtil(builder.build().findAllAttbs());
	}
	
	@Override
	public void reduce(final FormalConceptBuildingKey buildingKey, final Iterable<FormalConcept> concepts, final Context context) 
			throws IOException, InterruptedException 
	{
		
		for (final FormalConcept currentConcept : concepts){
			
			if (!currentConcept.extentIsEmpty() && buildingKey.canonicityTest(currentConcept)){
				
				final String nextBuildingAttribute = this.attributesUtil.nextAttribute(buildingKey.getBuildingAttribute());
				
				final FormalConceptBuildingKey nextKey = buildingKey.deriveNewBuildingKey(nextBuildingAttribute);
				
				context.write(nextKey, currentConcept);
				
				incrementGeneratedFormalConcepts(context);
			}			
		}
	}

	private void incrementGeneratedFormalConcepts(final Context context) {
		
		String counterGroupName = CounterDefinitions.FORMAL_CONCEPT_COUNTERS_GROUP.name();
		
		String formalConceptCounterName = CounterDefinitions.NUMBER_OF_GENERATED_FORMAL_CONCEPTS_COUNTER.name();
				
		final Counter formalConceptsCounter = context.getCounter(counterGroupName, formalConceptCounterName);
		
		formalConceptsCounter.increment(1);
	}
}
