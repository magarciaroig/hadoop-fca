package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.mgarciaroig.pfc.fca.analysis.action.formalconceptbuilding.error.UnableToWriteFornalConcept;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;
import com.mgarciaroig.pfc.fca.framework.oozie.BaseAction;

public abstract class FormalConceptFileCreationBase extends BaseAction {
	
	private final static String etlHdfsFormalContextHdfsPathProperty = "etlHdfsFormalContextHdfsPath";
	private final static String analysisDataPreparationHdfsPathProperty = "analysisDataPreparationHdfsPath";
	
	protected FormalContextItemRepository repository;

	protected FormalConceptFileCreationBase(String[] args) throws IOException {
		super(args);	
		
		final Path formalContextPath = new Path(super.getPropertyFromConfiguration(etlHdfsFormalContextHdfsPathProperty));
		final Path objectsIdsByAttributeBasePath = new Path(super.getPropertyFromConfiguration(analysisDataPreparationHdfsPathProperty));
						
		this.repository = new FormalContextItemRepository(super.getFileSystem(), formalContextPath, objectsIdsByAttributeBasePath);
	}
	
	protected void createFormalConceptFile(final Path toCreate, final FormalConceptBuildingKey key, final FormalConcept value) throws UnableToWriteFornalConcept{
		
		try {
			try (final Writer writer = SequenceFile.createWriter(
					super.getConfiguration(),
		            SequenceFile.Writer.file(toCreate),
		            SequenceFile.Writer.keyClass(FormalConceptBuildingKey.class),
		            SequenceFile.Writer.valueClass(FormalConcept.class))) 
		    {
								
				writer.append(key, value);
			}						
		}
		catch (final IOException writeFormalConceptError){
			
			throw new UnableToWriteFornalConcept(writeFormalConceptError);			
		}
		
	}

}
