package com.mgarciaroig.pfc.fca.etl.action.formalcontext;

import java.io.IOException;
import java.util.TreeMap;

import com.mgarciaroig.pfc.fca.etl.action.dataprepare.Field;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

class FormalContextKmeansFieldGenerator extends FormalContextFieldGeneratorBase {
	
	private static final int CLUSTER_FOR_NOT_DEFINED = -1;

	private final FileSystem fileSystem;
	
	private final String baseClustersDataPathName;
			
	FormalContextKmeansFieldGenerator(final FileSystem fileSystem, final String baseClustersDataPathName){
		
		super();
		
		this.fileSystem = fileSystem;
		this.baseClustersDataPathName = baseClustersDataPathName;				
	}	

	@Override
	TreeMap<Text, BooleanWritable> generateFormalContextFields(final Field field, final Writable fieldValue) throws IOException {
		
		final TreeMap<Text, BooleanWritable> formalContextFields = new TreeMap<>();
		
		final ClusteredDataRetriever clusterDataFinder = new ClusteredDataRetriever(fileSystem, buildFinalClusterPathForField(field));
						
		final Integer[] clusters = clusterDataFinder.retrieveClusters();			
		
		final Text fieldNameForNotDefined = buildFormalContextFieldName(field, buildUndefinedClusterSuffix());
		final BooleanWritable fieldValueForNotDefined = valueForField(fieldValue, NullWritable.get().toString());
		
		formalContextFields.put(fieldNameForNotDefined, fieldValueForNotDefined);
		
		final int clusterPoint = clusterFor(clusterDataFinder, fieldValue);		
		
		for (final int currentCluster : clusters){
			
			final Text fieldNameForCurrentCluster = buildFormalContextFieldName(field, buildStdClusterSuffix(currentCluster));			
			final BooleanWritable fieldValueForCurrentCluster = new BooleanWritable(clusterPoint == currentCluster ? true : false);						
			
			formalContextFields.put(fieldNameForCurrentCluster, fieldValueForCurrentCluster);			
		}				
		
		return formalContextFields;
	}

	private Integer clusterFor(final ClusteredDataRetriever clusterDataFinder, final Writable fieldValue) throws IOException {
		
		final Integer cluster;
		
		if (fieldValue instanceof NullWritable){
			cluster = CLUSTER_FOR_NOT_DEFINED;
		}
		else {
			cluster = clusterDataFinder.clusterFor(Double.valueOf(fieldValue.toString()));
		}
		
		return cluster;
	}
	
	private Path buildFinalClusterPathForField(final Field field){
		
		return new Path(baseClustersDataPathName.concat("/").concat(field.toString()).concat("-kmeans/clusteredPoints/part-m-00000"));
	}
	
	private String buildStdClusterSuffix(final Integer cluster){	
		
		return buildClusterSuffix(format(cluster));				
	}		
	
	private String buildUndefinedClusterSuffix(){	
		
		return buildClusterSuffix("UND");				
	}
	
	private String buildClusterSuffix(final String ending){
		return "CLUSTER-".concat(ending);
	}	
}
