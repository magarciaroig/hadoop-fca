package com.mgarciaroig.pfc.fca.etl.action.formalcontext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.mgarciaroig.pfc.fca.util.MaxSizedHashMap;
import org.apache.mahout.math.Vector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

class ClusteredDataRetriever {
	
	private static final int MAX_CACHED_POINTS = 1000;

	private interface ClustersDataProcessor {
		/**
		 * Id returns true, the searching process stops
		 * @param clusterNumber
		 * @param clusteredData
		 * @return
		 */
		public boolean processClusterData (final IntWritable clusterNumber, final WeightedPropertyVectorWritable clusteredData);
	}	
	
	final HashMap<Integer, MaxSizedHashMap<Double,Integer>> clustersAndPointsCache = new HashMap<>();
	
	private final FileSystem fileSystem;
	private final Path clustersDataPath;

	ClusteredDataRetriever (final FileSystem fileSystem, final Path clustersDataPath) {
		this.fileSystem = fileSystem;
		this.clustersDataPath = clustersDataPath;
	}
	
	Integer[] retrieveClusters() throws IOException{		
		
		final ClustersDataProcessor allClustersProcessor = new ClustersDataProcessor(){

			@Override
			public boolean processClusterData(final IntWritable clusterNumber, final WeightedPropertyVectorWritable clusteredData) {
								
				fillPointsCache(clusterNumber, clusteredData);
				
				return false;				
			}
		};
		
		processDataFile(allClustersProcessor);				
												
		return retrieveSortedClustersFromCache().toArray(new Integer[]{});
	}			
	
	Integer clusterFor(final Double point) throws IOException{
		
		final ClustersDataProcessor clustersForPointProcessor = new ClustersDataProcessor(){						

			@Override
			public boolean processClusterData(final IntWritable clusterNumber, final WeightedPropertyVectorWritable clusteredData) {
				
				fillPointsCache(clusterNumber, clusteredData);
				
				final Vector points = clusteredData.getVector();
								
				return searchPoint(points, point);
			}
		};
		
		processDataFile(clustersForPointProcessor);				
		
		return searchClusterForPoint(point);
	}
	
	private List<Integer> retrieveSortedClustersFromCache(){
		
		final List<Integer> allClusters = retrieveClustersFromCache();
		Collections.sort(allClusters);
		
		return allClusters;
	}
	
	private List<Integer> retrieveClustersFromCache(){
		
		final List<Integer> allClusters = new ArrayList<Integer>();
		
		for (int currentCluster : clustersAndPointsCache.keySet()){
			allClusters.add(currentCluster);
		}				
		
		return allClusters;
	}

	private Integer searchClusterForPoint(final Double point) {
		Integer clusterForPoint = null;
		
		for (final MaxSizedHashMap<Double, Integer> currentPointsCache : clustersAndPointsCache.values()){
			
			clusterForPoint = currentPointsCache.get(point);
			
			if (clusterForPoint != null) break;											
		}
		
		return clusterForPoint;
	}
	
	private void fillPointsCache(final IntWritable clusterNumber, final WeightedPropertyVectorWritable clusteredData) {
		
		final int currentCluster = clusterNumber.get();		
		
		final MaxSizedHashMap<Double,Integer> onlyPointsCache = pointsCacheForCluster(currentCluster);				
		
		final Vector points = clusteredData.getVector();
		
		for (int currentPointIndex = 0; currentPointIndex < points.size(); currentPointIndex++){
			
			final Double currentPoint = clusteredData.getVector().get(currentPointIndex);
			onlyPointsCache.put(currentPoint, clusterNumber.get());
		}																	
	}

	private MaxSizedHashMap<Double, Integer> pointsCacheForCluster(final int currentCluster) {		
		
		final MaxSizedHashMap<Double, Integer> onlyPointsCache;
						
		if (newClusterDiscovered(currentCluster)) {
			
			onlyPointsCache = new MaxSizedHashMap<Double,Integer>(MAX_CACHED_POINTS);
			clustersAndPointsCache.put(currentCluster, onlyPointsCache);
		}
		else {
			
			onlyPointsCache = clustersAndPointsCache.get(currentCluster);
		}
		
		return onlyPointsCache;
	}

	private boolean newClusterDiscovered(final int currentCluster) {
		return !clustersAndPointsCache.containsKey(currentCluster);
	}
	
	private boolean searchPoint(final Vector points, final Double pointToSearch) {
		
		boolean pointWasFound = false;
		
		for (int currentPointIndex = 0; currentPointIndex < points.size(); currentPointIndex++){
			
			final Double currentPoint = points.get(currentPointIndex);
			
			pointWasFound = currentPoint.equals(pointToSearch);
			if (pointWasFound) break;
		}	
		
		return pointWasFound;
	}
	
	private void processDataFile (final ClustersDataProcessor processor) throws IOException{
		
		final Option filePath = SequenceFile.Reader.file(clustersDataPath);
				
		try (final SequenceFile.Reader clusteredDataReader = new SequenceFile.Reader(fileSystem.getConf(), filePath))
		{
			final IntWritable cluster = new IntWritable();
	        
	        final WeightedPropertyVectorWritable point = new WeightedPropertyVectorWritable();
	        
	        boolean stop = false;
	        
	        while (!stop && clusteredDataReader.next(cluster, point)){
	        	stop = processor.processClusterData(cluster, point);
	        }
		}	
	}		
}
