package com.mgarciaroig.fca.etl.action.clusterization;

import java.io.IOException;

import com.mgarciaroig.fca.framework.error.ReactorsFCAAnalizerException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import com.mgarciaroig.fca.etl.action.dataprepare.Field;

/**
 * Canopy mahout clusterization action
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class CanopyClusterizationAction extends ClusterizationBaseAction {
		
	/**
	 * Main entry point
	 * @param args
	 * @throws IOException
	 * @throws ReactorsFCAAnalizerException
	 */
	public static void main(String[] args) throws IOException, ReactorsFCAAnalizerException{
		
		final CanopyClusterizationAction action = new CanopyClusterizationAction(args);
		action.run();
	}	

	private CanopyClusterizationAction(String[] args) throws IOException {
		super(args);		
	}
	
	@Override
	protected void clusterFieldValues(final Field field, final String inputPathName) throws ClassNotFoundException, IllegalArgumentException, IOException, InterruptedException {
		
		final String outputPathName = this.getPropertyFromConfiguration(ETL_HDFS_MAHOUT_CANOPY_PATH_NAME_PROPERTY);
		
		final Path mahoutInputPathForField = new Path(buildSpecificInputPathNameFor(inputPathName, field));						
		final Path canopyOutputPathForField = new Path(buildSpecificCanopyPathNameFor(outputPathName, field));
										
		final double canopyClusterizationMinThresold = getClusterClassificationThreshold();
		final DistanceMeasure euclideanDistance = new EuclideanDistanceMeasure();
		
		final Vector canopyThesoldValues = loadCanopyThresoldValuesFor(inputPathName, field);
		final double t1CanopyThresoldValue = getCanopyT1ThresoldValue(canopyThesoldValues);
		final double t2CanopyThresoldValue = getCanopyT2ThresoldValue(canopyThesoldValues);
		
		CanopyDriver.run(getConfiguration(), mahoutInputPathForField, canopyOutputPathForField, euclideanDistance, t1CanopyThresoldValue, t2CanopyThresoldValue, true, canopyClusterizationMinThresold, false);		
	}	
	
	private Vector loadCanopyThresoldValuesFor(final String baseInputPathName, final Field field) throws IOException {
		
		Vector thresoldCanopyValues = null;
		
		final Path canopyThresoldValuesFile = new Path(buildSpecificCanopyThresoldInputPathNameFor(baseInputPathName, field));
		
		final Option fileOptionPath = SequenceFile.Reader.file(canopyThresoldValuesFile);
		final Writable key = NullWritable.get();
		final VectorWritable value = new VectorWritable();
		
		try (final SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(getConfiguration(), fileOptionPath))
		{					
			while (sequenceFileReader.next(key, value)) {
				thresoldCanopyValues = value.get();
			}
		}
		
		return thresoldCanopyValues;
	}
	
	private double getCanopyT1ThresoldValue(Vector canopyThresoldValues){
		return canopyThresoldValues.get(0);
	}
	
	private double getCanopyT2ThresoldValue(Vector canopyThresoldValues){
		return canopyThresoldValues.get(1);
	}				
	
	private String buildSpecificCanopyThresoldInputPathNameFor(final String inputPathName, final Field field){
		return buildSpecificPathNameFor(inputPathName, field, ".canopy_thresold-r-00000");		
	}			
}
