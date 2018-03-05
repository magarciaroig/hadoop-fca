package com.mgarciaroig.fca.etl.action.clusterization;

import java.io.IOException;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;
import com.mgarciaroig.pfc.fca.etl.action.discretization.DiscretizationType;
import com.mgarciaroig.pfc.fca.etl.action.discretization.codifier.CodifierFilter;
import com.mgarciaroig.pfc.fca.framework.error.ReactorsFCAAnalizerException;
import com.mgarciaroig.pfc.fca.framework.oozie.BaseAction;

/**
 * Base class to implement both Canopy and Kmeans clusterization 
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public abstract class ClusterizationBaseAction extends BaseAction {
	
	protected static final String ETL_HDFS_MAHOUT_INPUT_PATH_NAME_PROPERTY = "etlHdfsMahoutInput";	
	protected static final String ETL_HDFS_MAHOUT_CANOPY_PATH_NAME_PROPERTY = "etlHdfsMahoutCanopyOutput";
	protected static final String ETL_HDFS_MAHOUT_KMEANS_PATH_NAME_PROPERTY = "etlHdfsMahoutKmeansOutput";
		
	protected ClusterizationBaseAction(String[] args) throws IOException {
		super(args);		
	}

	@Override
	protected void run() throws ReactorsFCAAnalizerException {
		final String etlHdfsMahoutInputPathName = this.getPropertyFromConfiguration(ETL_HDFS_MAHOUT_INPUT_PATH_NAME_PROPERTY);		
			
		
		try {
			final CodifierFilter codifierFilter = new CodifierFilter(this.getConfiguration(), DiscretizationType.KMEANS);
			
			for (final Field currentField : Field.values()){
				
				if (codifierFilter.isWanted(currentField.toString())){
					clusterFieldValues (currentField, etlHdfsMahoutInputPathName);
				}
			}
			
		
		} catch (IOException|ClassNotFoundException|IllegalArgumentException|InterruptedException e) {
			
			throw new ReactorsFCAAnalizerException(e);
		}
		
	}
	
	protected String buildSpecificPathNameFor(final String basePathName, final Field field, final String suffix){
		return basePathName.concat("/").concat(field.toString()).concat(suffix);
	}
	
	protected String buildSpecificInputPathNameFor(final String inputPathName, final Field field){
		return buildSpecificPathNameFor(inputPathName, field, "-r-00000");		
	}
	
	protected String buildSpecificCanopyPathNameFor(final String outputPathName, final Field field){
		return buildSpecificPathNameFor(outputPathName, field, "-centroids");		
	}
	
	protected String buildSpecificKmeansPathNameFor(final String pathName, final Field field){
		return buildSpecificPathNameFor(pathName, field, "-kmeans");
	}
	
	protected double getClusterClassificationThreshold(){
		
		final double clusterizationMinThresold = 0.00001;
		
		return clusterizationMinThresold;
	}
	
	protected abstract void clusterFieldValues(final Field field, final String inputPathName) 
			throws ClassNotFoundException, IllegalArgumentException, IOException, InterruptedException ;
}
