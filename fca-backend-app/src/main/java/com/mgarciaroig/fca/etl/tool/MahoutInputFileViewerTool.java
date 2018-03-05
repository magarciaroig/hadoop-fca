package com.mgarciaroig.fca.etl.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.VectorWritable;

/**
 * Tool to visualize mahout clusterization input files
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class MahoutInputFileViewerTool extends Tool {

	/**
	 * Main entry point
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if (args.length != 1){
			showUsage();
			System.exit(1);
		}
		
		final Tool tool = new MahoutInputFileViewerTool();
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
            	            	
            	final VectorWritable record = (VectorWritable) value;
            	
            	System.out.println(value.toString());
            	            	            	
            	System.out.println("==========================");
            }
        } finally {
            IOUtils.closeStream(sequenceFileReader);
        }
		
	}
	
	private static void showUsage(){
		
		showUsage(EtlHdfsFileViewerTool.class, "[hdfs sequence file]");				
	}

}
