package com.mgarciaroig.fca.etl.action.dataimport;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class responsible for importing the remote data file into HDFS cluster
 * 
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
class DataRetriever {
	
	private FileSystem fileSystem;

	DataRetriever(final FileSystem fileSystem){
		this.fileSystem = fileSystem;
	}
	
	/**
	 * Retrieves the remote file and upload into into cluster
	 * 
	 * @param inputDataUrl Remote file to import
	 * @param outputPath Path in the cluster when the field should be saved
	 * @param outputFileName Final file name in the cluster
	 * @throws UnableToRetrieveInputDataReactorsFromIAEAException
	 */
	void retrieveFileAndUploadItToCluster(final URL inputDataUrl, final Path outputPath, final String outputFileName) throws UnableToRetrieveInputDataReactorsFromIAEAException {
		
		try {
	    
			retrieveFileAndUploadItToClusterWithoutErrorChecking(inputDataUrl, outputPath, outputFileName);	
		}
		catch (Exception e){
			throw new UnableToRetrieveInputDataReactorsFromIAEAException(inputDataUrl, outputPath, outputFileName, e);
		}
	}

	private void retrieveFileAndUploadItToClusterWithoutErrorChecking(final URL inputDataUrl, final Path outputPath, final String outputFileName)
			throws IOException {
											
		
		InputStream connStream = null;
		FSDataOutputStream outStream = null;
		
		try {
			connStream = read(inputDataUrl);
			outStream = fileSystem.create(new Path(outputPath, outputFileName));
		}
		finally {
			if (connStream != null) connStream.close();
			if (outStream != null) outStream.close();
		}
	}
	
	private InputStream read(URL toBeRead) throws IOException {
		
		final URLConnection conn = toBeRead.openConnection();
		
		if (conn instanceof HttpURLConnection){
						
			final String httpUserAgentRequestPropertyName = "User-Agent";
			final String mozillaUserAgent = "Mozilla/4.0";
			
			final HttpURLConnection httpcon = (HttpURLConnection) conn;		
			
			httpcon.addRequestProperty(httpUserAgentRequestPropertyName, mozillaUserAgent);
			
		}

		return conn.getInputStream();		 
	}
}
