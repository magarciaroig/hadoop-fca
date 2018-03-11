package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import com.mgarciaroig.fca.analysis.action.formalconceptbuilding.error.UnableToReadFormalContext;

import com.mgarciaroig.fca.analysis.model.FormalConcept;
import com.mgarciaroig.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.fca.framework.error.FCAAnalizerException;

/**
 * Action to create the data corresponding to the minimal node in the concept lattice graph. We need to generate it explicitly because
 * of is is not being computed in the fca algorythm
 * @author Miguel �?ngel García Roig (rocho08@gmail.com)
 *
 */
public class MinimalFormalConceptCreationAction extends FormalConceptFileCreationBase {
	
	private final static String analysisFcaMinConceptHdfsPathProperty = "analysisFcaMinConceptHdfsPath";
	private final static String analysisFcaMinConceptFileName = "min_concept.seq";
	
	private final Path outputSeqFile;
	
	public static void main(String[] args) throws IOException, FCAAnalizerException {
		
		final MinimalFormalConceptCreationAction minFormalConceptCreatorAction = new MinimalFormalConceptCreationAction(args);				
		
		minFormalConceptCreatorAction.run();
	}

	private MinimalFormalConceptCreationAction(String[] args) throws IOException {
		
		super(args);	
		
		this.outputSeqFile = new Path(super.getPropertyFromConfiguration(analysisFcaMinConceptHdfsPathProperty), analysisFcaMinConceptFileName);
	}

	@Override
	protected void run() throws FCAAnalizerException {		
		
		final FormalConceptBuildingKey initialKey;
		final FormalConcept initialConcept;
		
		try {
			initialKey = createEmptyKey();
			initialConcept = createMinimumFormalConcept();
		}
		catch (final IOException readFromFormalContextError){
			
			throw new UnableToReadFormalContext(readFromFormalContextError);
		}

		createFormalConceptFile(this.outputSeqFile, initialKey, initialConcept);
	}

	private FormalConcept createMinimumFormalConcept() throws IOException {				
		
		return new FormalConcept(new ArrayList<String>(), this.repository.findAllAttbs());
	}

	private FormalConceptBuildingKey createEmptyKey() {
		
		return new FormalConceptBuildingKey();
	}

}
