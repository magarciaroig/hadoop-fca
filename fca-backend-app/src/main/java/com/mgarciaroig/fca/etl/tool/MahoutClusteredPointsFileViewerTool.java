package com.mgarciaroig.fca.etl.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Tool to visualize mahout clusterization output tools
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class MahoutClusteredPointsFileViewerTool extends Tool {
	
	public static void main(final String[] args) throws IOException{
		
		if (args.length != 1){
			showUsage();
			System.exit(1);
		}
		
		final Tool tool = new MahoutClusteredPointsFileViewerTool();
		tool.run(args);	
	}

	@Override
	protected void run(String... args) throws IOException {
		
		final String sequenceFilePath = args[0];
		
		final Configuration conf = new Configuration();
        final Option filePath = SequenceFile.Reader.file(new Path(sequenceFilePath));
        final SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath);

        final Writable key = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getKeyClass(), conf);
        final Writable value = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getValueClass(), conf);

        try {

            while (sequenceFileReader.next(key, value)) {
            	
            	System.out.println(String.format("'%s' belongs to cluster '%s'", value.toString(), key.toString()));
            	            	            	
            }
        } finally {
            IOUtils.closeStream(sequenceFileReader);
        }
		
		
	}
	
	private static void showUsage(){
		
		showUsage(MahoutClusteredPointsFileViewerTool.class, "[hdfs sequence file]");				
	}

}
