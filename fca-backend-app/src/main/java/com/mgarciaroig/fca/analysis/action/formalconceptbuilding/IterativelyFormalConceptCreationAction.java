package com.mgarciaroig.fca.analysis.action.formalconceptbuilding;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.mgarciaroig.fca.analysis.action.formalconceptbuilding.error.FCAAlgorythmDoesNotConvergeException;
import com.mgarciaroig.fca.analysis.action.formalconceptbuilding.error.FCAFilesCreationException;
import com.mgarciaroig.fca.analysis.action.formalconceptbuilding.error.FCAMapReduceProcessingJobException;

import com.mgarciaroig.fca.analysis.model.FormalConcept;
import com.mgarciaroig.fca.analysis.model.FormalConceptBuildingKey;
import com.mgarciaroig.fca.framework.error.FCAAnalizerException;
import com.mgarciaroig.fca.framework.oozie.BaseAction;

/**
 * Action to iteratively launch map-reduce actions until all formal concepts become generated
 * @author Miguel �?ngel García Roig (rocho08@gmail.com)
 *
 */
public class IterativelyFormalConceptCreationAction extends BaseAction {
	
	private final static String analysisFcaMaxConceptHdfsPathProperty = "analysisFcaMaxConceptHdfsPath";
	private final static String analysisFcaIterationsHdfsPathProperty = "analysisFcaIterationsHdfsPath";	
	private final static String analysisFcaMaxIterationsProperty = "analysisFcaMaxIterations";
	
	private final Path iterationsBasePath;
	private final Path iterationsInitialPath;
	private final int maxIterations;
	
	private final static Logger logger = Logger.getLogger(IterativelyFormalConceptCreationAction.class);
	
	public static void main(String[] args) throws IOException, FCAAnalizerException{
		
		final IterativelyFormalConceptCreationAction action = new IterativelyFormalConceptCreationAction(args);
		
		action.run();		
	}

	private IterativelyFormalConceptCreationAction(String[] args) throws IOException {
		super(args);
		
		this.iterationsBasePath = new Path(super.getPropertyFromConfiguration(analysisFcaIterationsHdfsPathProperty));
		this.iterationsInitialPath = new Path(super.getPropertyFromConfiguration(analysisFcaMaxConceptHdfsPathProperty));
		this.maxIterations = Integer.valueOf(super.getPropertyFromConfiguration(analysisFcaMaxIterationsProperty));
	}

	@Override
	protected void run() throws FCAAnalizerException {
						
		try {
		
			int iterationNumber = 0;			
			boolean allConceptsHaveBeeenGenerated = false;
			
			Path inputPath = this.iterationsInitialPath;
			Path outputPath = nextOutputPath(iterationNumber);
														
			while (!allConceptsHaveBeeenGenerated && iterationNumber < maxIterations){															
				
				logInfo(String.format("Starting iteration %d, input '%s' output '%s'", iterationNumber, inputPath.toString(), outputPath.toString()));
				
				final Job job = createMapReduceJob(inputPath, outputPath);
																		
				launchAndWaitUntilCompletion(job);								
				
				allConceptsHaveBeeenGenerated = generatedFormalConcepts(job) == 0;								
				
				resetGeneratedFormalConceptsCounter(job);			
				
				iterationNumber++;
				
				inputPath = outputPath;
				outputPath = nextOutputPath(iterationNumber);								
			}
			
			if (!allConceptsHaveBeeenGenerated){
								
				throw new FCAAlgorythmDoesNotConvergeException(maxIterations);
			}				
			
		}
		catch (final IOException ioError){
			
			throw new FCAFilesCreationException(ioError);
		}		
	}

	private void launchAndWaitUntilCompletion(final Job job) throws IOException, FCAAnalizerException {
		
		try {
			job.waitForCompletion(true);
			
			if (!job.isSuccessful()){				
				throw new FCAMapReduceProcessingJobException();
			}
		
		} catch (final ClassNotFoundException | InterruptedException jobProcessError) {
						
			throw new FCAMapReduceProcessingJobException(jobProcessError);
		}
		
		logInfo(String.format("The number of generated formal concepts was %d", generatedFormalConcepts(job)));
	}
	
	private long generatedFormalConcepts(final Job job) throws IOException{
		return findGeneratedFormalConceptsCounter(job).getValue();
	}

	private void resetGeneratedFormalConceptsCounter(final Job job) throws IOException {
		findGeneratedFormalConceptsCounter(job).setValue(0);		
	}

	private Counter findGeneratedFormalConceptsCounter(final Job job) throws IOException {
				
		String groupName = CounterDefinitions.FORMAL_CONCEPT_COUNTERS_GROUP.name();
		String counterName = CounterDefinitions.NUMBER_OF_GENERATED_FORMAL_CONCEPTS_COUNTER.name();
		
		return job.getCounters().findCounter(groupName, counterName);
	}
	
	private Job createMapReduceJob(final Path inputPath, final Path outputPath) throws IOException{
		
		final Job job = Job.getInstance(getConfiguration(), IterativelyFormalConceptCreationAction.class.getSimpleName());
		job.setJarByClass(IterativelyFormalConceptCreationAction.class);
					
		SequenceFileInputFormat.addInputPath(job, inputPath);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
					
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
								
		job.setMapperClass(FormalConceptBuildingMapper.class);			
		job.setMapOutputKeyClass(FormalConceptBuildingKey.class);		
		job.setMapOutputValueClass(FormalConcept.class);			
					
		job.setReducerClass(FormalConceptBuildingReducer.class);									
		job.setOutputKeyClass(FormalConceptBuildingKey.class);
		job.setOutputValueClass(FormalConcept.class);			
						
		return job;
	}
		
	private Path nextOutputPath(final int iterationNumber) {
		
		final String outputPathDirectoryName = "iteration-".concat(String.valueOf(iterationNumber));
		
		return new Path(this.iterationsBasePath, outputPathDirectoryName);
	}
	
	private void logInfo(final String msg){
		if (logger.isInfoEnabled()){
			logger.info(msg);
		}
	}
}
