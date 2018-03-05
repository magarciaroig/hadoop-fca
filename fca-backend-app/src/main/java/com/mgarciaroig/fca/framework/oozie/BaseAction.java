package com.mgarciaroig.fca.framework.oozie;

import java.io.IOException;

import com.mgarciaroig.pfc.fca.framework.error.FCAAnalizerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Base class for all oozie actions
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
public abstract class BaseAction {
	
	private final Configuration config;
	
	private final String[] args;
				
	protected BaseAction(final String args[]) throws IOException{
						
		this.args = args;
		this.config = readConfiguration();
	}
	
	/**
	 * Reads configuration passed by oozie properties
	 * @return
	 */
	private Configuration readConfiguration(){				
		
		final String oozieActionConfigPropertyName = "oozie.action.conf.xml";
		
		final Configuration config = new Configuration();
						
		final String oozieConfigPathName = System.getProperty(oozieActionConfigPropertyName);		
		
		if (oozieConfigPathName != null){
						
			config.addResource(new Path(oozieConfigPathName));
		}				
		
		return config;
	}
	
	protected Configuration getConfiguration(){						
		return this.config;
	}	
	
	protected FileSystem getFileSystem(){
		final FileSystem fileSystem;		
		
		try {			
			fileSystem = FileSystem.get(getConfiguration());
		} catch (IOException e) {
			
			throw new RuntimeException(e);
		}
		
		return fileSystem;
	}
	
	protected String getPropertyFromConfiguration(final String propertyName){
		
		return getConfiguration().get(propertyName);
	}
				

	protected String[] getArgs() {
		return args;
	}	
	
	protected abstract void run() throws FCAAnalizerException;
}
