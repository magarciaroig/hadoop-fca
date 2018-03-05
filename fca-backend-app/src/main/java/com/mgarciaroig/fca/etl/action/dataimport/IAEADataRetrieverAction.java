package com.mgarciaroig.fca.etl.action.dataimport;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
//import org.apache.log4j.Logger;

import com.mgarciaroig.pfc.fca.framework.error.ReactorsFCAAnalizerException;
import com.mgarciaroig.pfc.fca.framework.oozie.BaseAction;

/**
 * Java action to be called from oozie. It imports the IAEA reactors file and puts it inside the HDFS cluster
 * 
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
public class IAEADataRetrieverAction extends BaseAction {
	
	private static final String REACTOR_RESEARCH_FILENAME_CONF_PROPERTY = "dataImport.IAEAFileName";
	private static final String IAEA_OUTPUT_DATA_PATH_NAME_CONF_PROPERTY = "dataImport.IAEAOutputDataPathName";
	private static final String IAEA_INPUT_DATA_URL_CONF_PROPERTY = "dataImport.IAEAinputDataUrl";
	
	private static final Logger logger = Logger.getLogger(IAEADataRetrieverAction.class);

	/**
	 * Main entry point
	 * @param args
	 * @throws ReactorsFCAAnalizerException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ReactorsFCAAnalizerException, IOException{
													
		final IAEADataRetrieverAction dataRetrieverAction = new IAEADataRetrieverAction(args);
		
		dataRetrieverAction.run();		
	}

	private IAEADataRetrieverAction(String[] args) throws IOException {
		super(args);
	}

	/**
	 * Performs the action
	 */
	public void run() throws ReactorsFCAAnalizerException{
																								 			
		final FileSystem fileSystem = getFileSystem();
		final DataRetriever retriever = new DataRetriever(fileSystem);
		
		final String inputDataUrlName = getPropertyFromConfiguration(IAEA_INPUT_DATA_URL_CONF_PROPERTY);
		final String IAEAOutputDataPathName = getPropertyFromConfiguration(IAEA_OUTPUT_DATA_PATH_NAME_CONF_PROPERTY);
		final String IAEAOutputFileName = getPropertyFromConfiguration(REACTOR_RESEARCH_FILENAME_CONF_PROPERTY);
		
		final URL inputDataURL = createInputDataURL(inputDataUrlName);				
		final Path IAEAOutputDataPath = new Path(IAEAOutputDataPathName);
		
		retriever.retrieveFileAndUploadItToCluster(inputDataURL, IAEAOutputDataPath, IAEAOutputFileName);
	}

	private URL createInputDataURL(final String inputDataUrl) throws BadIAEADateImportUrlException {
			
		final URL url;
		
		try {
			url = new URL(inputDataUrl);
		}
		catch (MalformedURLException e){
			throw new BadIAEADateImportUrlException(inputDataUrl, e);
		}
		
		return url;
	}

}