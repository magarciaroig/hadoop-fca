package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Throwables;

import com.mgarciaroig.pfc.fca.analysis.model.AttributesUtil;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptNonConsistentAttributesException;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

/**
 * Mapper in charge of generating derived formal concepts
 * @author Miguel �?ngel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class FormalConceptBuildingMapper extends Mapper<FormalConceptBuildingKey,FormalConcept,FormalConceptBuildingKey,FormalConcept> {
			
	protected FormalContextItemRepository repository;
	protected AttributesUtil attributesUtil;	
			
	@Override
	public void setup(final Context context) throws IOException{
		
		final Configuration conf = context.getConfiguration();
		
		final FileSystem fs = FileSystem.get(conf);
		
		final RepositoryBuilder builder = new RepositoryBuilder(conf, fs);
		
		this.repository = builder.build();	
		this.attributesUtil = new AttributesUtil(this.repository.findAllAttbs());
	}
	
	@Override
	public void map (final FormalConceptBuildingKey buildingKey, final FormalConcept currentFormalConcept, final Context context) throws IOException, InterruptedException {
						
		final List<String> pendingAttributes = attributesUtil.attributesFrom(buildingKey.getBuildingAttribute());				
		
		for (final String currentAttb : pendingAttributes){									
			
			if (!currentFormalConcept.hasAttb(currentAttb)){
				
				try {
					
					final FormalConceptBuildingKey derivedKey = currentFormalConcept.deriveNewBuildingKey(currentAttb);
					final FormalConcept derivedConcept = currentFormalConcept.deriveNewFormalConcept(this.repository, currentAttb);
					
					context.write(derivedKey, derivedConcept);
				
				} catch (final FormalConceptNonConsistentAttributesException e) {
					
					Throwables.propagate(e);										
				}				
			}						
		}		
	}

}
