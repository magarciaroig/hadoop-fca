package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.mgarciaroig.pfc.fca.analysis.persistence.FormalContextItemRepository;

class RepositoryBuilder {
		
	private static final String etlHdfsFormalContextHdfsPathProperty = "etlHdfsFormalContextHdfsPath";
	
	private static final String analysisDataPreparationHdfsPathProperty = "analysisDataPreparationHdfsPath";
	
	private final Configuration config;
	private final FileSystem fileSystem;
		
	RepositoryBuilder (final Configuration config, final FileSystem fileSystem){
		
		this.config = config;
		this.fileSystem = fileSystem;
	}
	
	FormalContextItemRepository build(){
					
		final Path formalContextPath = new Path(config.getTrimmed(etlHdfsFormalContextHdfsPathProperty));
		final Path objectsIdsByAttributeBasePath = new Path(config.getTrimmed(analysisDataPreparationHdfsPathProperty));
		
		return new FormalContextItemRepository(fileSystem, formalContextPath, objectsIdsByAttributeBasePath);		
	}		
}
