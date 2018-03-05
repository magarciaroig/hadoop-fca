package com.mgarciaroig.fca.etl.action.clusterization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;
import com.mgarciaroig.pfc.fca.framework.error.ReactorsFCAAnalizerException;

/**
 * Kmeans mahout clusterization action
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public class KmeansClusterizationAction extends ClusterizationBaseAction {
	
	/**
	 * Main entry point
	 * @param args
	 * @throws IOException
	 * @throws ReactorsFCAAnalizerException
	 */
	public static void main(String[] args) throws IOException, ReactorsFCAAnalizerException{
		
		final KmeansClusterizationAction action = new KmeansClusterizationAction(args);
		action.run();
	}

	private final List<String> kmeansParameterList = new ArrayList<String>();

	private KmeansClusterizationAction(String[] args) throws IOException {
		super(args);
	}

	@Override
	protected void clusterFieldValues(Field field, String inputPathName)
			throws ClassNotFoundException, IllegalArgumentException,
			IOException, InterruptedException {

		final String canopyCentroidsInputPathBaseName = this.getPropertyFromConfiguration(ETL_HDFS_MAHOUT_CANOPY_PATH_NAME_PROPERTY);
		
		final String kmeansOutputPathBaseName = this.getPropertyFromConfiguration(ETL_HDFS_MAHOUT_KMEANS_PATH_NAME_PROPERTY);

		final String mahoutInputPathNameForField = buildSpecificInputPathNameFor(inputPathName, field);
		
		final String canopyCentroidsInputPathName = buildSpecificCanopyPathNameFor(canopyCentroidsInputPathBaseName, field);
		
		final String kmeansOutputPathName = buildSpecificKmeansPathNameFor(kmeansOutputPathBaseName, field);
		
		kmeansParameterList.clear();

		addInputPathParameter(mahoutInputPathNameForField);
		addCentroidsInputPathParameter(canopyCentroidsInputPathName);
		addOutputPathParameter(kmeansOutputPathName);
		addDistanceClassNameParameter();
		addKParameter(field.getKmeansKParameter());
		addMaxIterationsParameter();
		addOverwriteOutputParameter();
		addClusteringParameter();

		try {
			executeKMeans();
		} catch (Exception e) {
			throw new IOException(String.format(
					"Error executing kmeans process for field '%s'",
					field.toString()), e);
		}
	}

	private void executeKMeans() throws Exception {

		final String[] kmeansArguments = kmeansParameterList.toArray(new String[] {});

		final KMeansDriver driver = new KMeansDriver();
		driver.run(kmeansArguments);
	}

	private void addInputPathParameter(final String inputPathName) {
		final String parameterName = "-i";

		kmeansParameterList.add(parameterName);
		kmeansParameterList.add(inputPathName);
	}

	private void addCentroidsInputPathParameter(
			final String centroidsInputPathName) {
		final String parameterName = "-c";

		kmeansParameterList.add(parameterName);
		kmeansParameterList.add(centroidsInputPathName);
	}

	private void addOutputPathParameter(final String outputPathName) {
		final String parameterName = "-o";

		kmeansParameterList.add(parameterName);
		kmeansParameterList.add(outputPathName);
	}

	private void addDistanceClassNameParameter() {

		final String parameterName = "-dm";
		final String distanceClassName = EuclideanDistanceMeasure.class.getName();

		kmeansParameterList.add(parameterName);
		kmeansParameterList.add(distanceClassName);
	}

	private void addKParameter(final int k) {
		final String parameterName = "-k";

		kmeansParameterList.add(parameterName);
		kmeansParameterList.add(String.valueOf(k));
	}

	private void addMaxIterationsParameter() {
		final String parameterName = "-x";
		final int maxIterations = 3;

		kmeansParameterList.add(parameterName);
		kmeansParameterList.add(String.valueOf(maxIterations));
	}

	private void addOverwriteOutputParameter() {
		final String parameterName = "-ow";

		kmeansParameterList.add(parameterName);
	}

	private void addClusteringParameter() {
		final String parameterName = "-cl";

		kmeansParameterList.add(parameterName);
	}
}
