package com.mgarciaroig.pfc.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;

import com.mgarciaroig.pfc.fca.analysis.action.formalconceptbuilding.error.UnableToReadFormalContext;

import com.mgarciaroig.pfc.fca.analysis.model.FormalConcept;
import com.mgarciaroig.pfc.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;

/**
 * Action to create the initial data for fca computation. Initial data corresponds to the maximum node in the concept lattice graph
 * @author Miguel �?ngel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class InitialFormalConceptFileCreationAction extends FormalConceptFileCreationBase {
			
	private final static String analysisFcaMaxConceptHdfsPathProperty = "analysisFcaMaxConceptHdfsPath";
	private final static String analysisFcaMaxConceptFileName = "max_concept.seq";
	
	private final Path outputSeqFile;
	
	
	public static void main(String[] args) throws IOException, FCAAnalizerException {
		
		final InitialFormalConceptFileCreationAction initialFormalConceptCreatorAction = new InitialFormalConceptFileCreationAction(args);				
		
		initialFormalConceptCreatorAction.run();
	}	

	private InitialFormalConceptFileCreationAction(String[] args) throws IOException {
		
		super(args);
		
		this.outputSeqFile = new Path(super.getPropertyFromConfiguration(analysisFcaMaxConceptHdfsPathProperty), analysisFcaMaxConceptFileName);
	}	

	@Override
	protected void run() throws FCAAnalizerException {	
		
		final FormalConceptBuildingKey initialKey;
		final FormalConcept initialConcept;
		
		try {
			initialKey = createInititialKey();
			initialConcept = createMaximunFormalConcept();
		}
		catch (final IOException readFromFormalContextError){
			
			throw new UnableToReadFormalContext(readFromFormalContextError);
		}
		
		createFormalConceptFile(this.outputSeqFile, initialKey, initialConcept);				
	}
	
	private FormalConceptBuildingKey createInititialKey() throws IOException{
		
		final String firstAttb = this.repository.findAllAttbs().get(0);
		
		return new FormalConceptBuildingKey(new TreeSet<String>(), firstAttb);		
	}
	
	private FormalConcept createMaximunFormalConcept() throws IOException {
					
		return new FormalConcept(this.repository.findAllObjectIds(), new ArrayList<String>());				
	}

}
