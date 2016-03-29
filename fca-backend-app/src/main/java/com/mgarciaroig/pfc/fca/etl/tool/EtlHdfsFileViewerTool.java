package com.mgarciaroig.pfc.fca.etl.tool;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Tool to view sequence files from hdfs
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class EtlHdfsFileViewerTool extends Tool {
	
	

	/** 
	 * Tool entry point
	 * @param args Hdfs route
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if (args.length != 1){
			showUsage();
			System.exit(1);
		}
		
		final Tool tool = new EtlHdfsFileViewerTool();
		tool.run(args);							
	}
	
	
	@Override
	protected void run(String... args) throws IOException{
		
		final String sequenceFilePath = args[0];
		
		final Configuration conf = new Configuration();
		final Option filePath = SequenceFile.Reader.file(new Path(sequenceFilePath));
		final SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf, filePath);

		final Writable key = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getKeyClass(), conf);
		final Writable value = (Writable) ReflectionUtils.newInstance(sequenceFileReader.getValueClass(), conf);

        try {

            while (sequenceFileReader.next(key, value)) {
            	
            	System.out.println(String.format("[ %s ]", key.toString()));
            	            	
            	final SortedMapWritable record = (SortedMapWritable) value;
            	
            	final Set<Entry<WritableComparable, Writable>> contents = record.entrySet();
            	
            	for (final Entry<WritableComparable, Writable> currentField : contents){
            		System.out.println(String.format("'%s': '%s'", currentField.getKey().toString(), currentField.getValue().getClass().equals(NullWritable.class) ? "null" : currentField.getValue().toString()));
            	}
            	
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
