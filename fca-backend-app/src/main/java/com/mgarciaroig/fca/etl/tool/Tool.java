package com.mgarciaroig.fca.etl.tool;

import java.io.IOException;

/**
 * Base class to implement tools
 * @author Miguel Ángel García Roig (rocho08@gmail.com)
 *
 */
abstract class Tool {

	protected static String buildToolCommandLine(final Class<?> toolType, final String arguments){
		return String.format("hadoop jar pfc-fca-1.0-SNAPSHOT.jar %s %s", toolType.getName(), arguments);
	}
	
	protected static void showUsage(final Class<?> toolType, final String arguments){
		
		System.out.println("Usage: ".concat(buildToolCommandLine(toolType, arguments)));
	}
	
	/**
	 * Perform the execution
	 * @param args
	 * @throws IOException
	 */
	protected abstract void run(String... args) throws IOException;
}
