package com.mgarciaroig.fca.etl.action.dataimport;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class DataRetrieverTest {
		
	private static final String FILE_PROTOCOL_PREFIIX = "file:///";
	private static final String BASE_TEST_FOLDER = "target/test-classes/testFiles/DataRetrieverTest";
			
	private static final String FILE_NAME = "input_file.txt";
	private static final String SOURCE_PATH_NAME = BASE_TEST_FOLDER.concat("/source/").concat(FILE_NAME);	
	private static final String TARGET_PATH_NAME = BASE_TEST_FOLDER.concat("/target");
	
	private Configuration configuration;
	private FileSystem fileSystem;
	
	@Before
	public void setUp() throws IOException{
		configuration = new Configuration();
		fileSystem = FileSystem.get(configuration);
		
		final Path targetPath = new Path(TARGET_PATH_NAME);		
		
		createEmptyPath(targetPath);				
	}
	
	@After
	public void setDown() throws IOException{
		final Path targetPath = new Path(TARGET_PATH_NAME);
		deletePathIfExixts(targetPath);
	}
	
	@Ignore
	@Test
	public void retrieveFileAndUploadItToClusterTest() throws UnableToRetrieveInputDataReactorsFromIAEAException, IOException{
		
		final DataRetriever retriever = new DataRetriever(fileSystem);
						
		final String inputURLPathName = buildInputURLPathName();
		
		final URL inputURL = new URL(inputURLPathName);			
		final Path outputPath = new Path(TARGET_PATH_NAME);
		 
		
		retriever.retrieveFileAndUploadItToCluster(inputURL, outputPath, FILE_NAME);
		
		final Path expectedPath = new Path(outputPath, FILE_NAME);
		
		assertTrue("Expected remote file to be uploaded in cluster", fileSystem.exists(expectedPath));
	}

	private String buildInputURLPathName() {
			
		return FILE_PROTOCOL_PREFIIX.concat(getCurrentPathName()).concat("/").concat(SOURCE_PATH_NAME);
	}
	
	private void createEmptyPath(final Path targetPath) throws IOException {
		deletePathIfExixts(targetPath);
		fileSystem.mkdirs(targetPath);
	}

	private void deletePathIfExixts(final Path targetPath) throws IOException {
		if (fileSystem.exists(targetPath)){
			fileSystem.delete(targetPath, true);
		}
	}
	
	private String getCurrentPathName(){						
		return System.getProperty("user.dir");
	}

}
