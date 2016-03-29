package com.mgarciaroig.pfc.fca.etl.action.dataprepare;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.mgarciaroig.pfc.fca.framework.error.ReactorsFCAAnalizerException;
import com.mgarciaroig.pfc.fca.framework.oozie.BaseAction;

public class IAEADataPrepareAction extends BaseAction {
			
	private static final String RAW_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY = "dataImport.RawIAEAReactorResearchFilePathName";
	private static final String IMPORTED_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY = "dataImport.ImportedIAEAReactorResearchFilePathName";
	
	private final Logger logger = Logger.getLogger(IAEADataPrepareAction.class);
		
	private final IAEADataFieldExtractor dataExtractor;	
	private final IAEAMergedRectorsDocumentCreator documentCreator;
			
	public static void main(String[] args) throws ReactorsFCAAnalizerException, IOException {
						
		IAEADataPrepareAction action = new IAEADataPrepareAction(args);
		action.run();
	}

	protected IAEADataPrepareAction(String[] args) throws IOException {
		super(args);				
		
		final IAEADataFieldExtractorBuilder dataExtractorBuilder = new IAEADataFieldExtractorBuilder();
		final IAEAMergedRectorsDocumentCreatorBuilder documentCreatorBuilder = new IAEAMergedRectorsDocumentCreatorBuilder();
		
		dataExtractor = dataExtractorBuilder.buildFieldExtractor();
		documentCreator = documentCreatorBuilder.buildDocumentCreator();		
	}		

	@Override
	protected void run() throws ReactorsFCAAnalizerException {
		
		try {			
						
			runWithoutErrorChecking();												
		
		} catch (FileNotFoundException e) {			
			throw new IAEADataPrepareActionException(e);
		}
		catch (IOException e){
			throw new IAEADataPrepareActionException(e);
		}		
	}

	private void runWithoutErrorChecking() throws IOException {
						
		final String reactorsDocumentToImportPathName = super.getPropertyFromConfiguration(RAW_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY);
		final String importedReactorsDocumentPathName = super.getPropertyFromConfiguration(IMPORTED_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY);
		
		final Path reactorsDocumentToImportPath = new Path(reactorsDocumentToImportPathName);
		final Path importedReactorsDocumentPath = new Path(importedReactorsDocumentPathName);
		
		HSSFWorkbook workbook = null;			
		OutputStream importedDocumentStream = null;
		
		try {
			
			final FileSystem fileSystem = super.getFileSystem();
			
			final List<IAEAExtractedReactorData> reactors = importData(fileSystem, reactorsDocumentToImportPath);
																							
			final Workbook mergedRectorsDataDoc = createMergedDocument(reactors);
			
			importedDocumentStream = persistMergedDocument(mergedRectorsDataDoc, fileSystem, importedReactorsDocumentPath);												
		}
		finally {
			if (workbook != null) workbook.close();
			
			if (importedDocumentStream != null) importedDocumentStream.close();				
		}
	}
	
	private OutputStream persistMergedDocument(final Workbook mergedRectorsDataDoc, final FileSystem fileSystem, 
			final Path importedReactorsDocumentPath) throws IOException 
	{
		
		final OutputStream importedDocumentStream = fileSystem.create(importedReactorsDocumentPath, true);
		mergedRectorsDataDoc.write(importedDocumentStream);	
		
		logger.info(String.format("Unique document with all rector merged data persisted to '%s'", importedReactorsDocumentPath));
		
		return importedDocumentStream;
	}

	private Workbook createMergedDocument(final List<IAEAExtractedReactorData> reactors) {
		
		logger.info("Creating unique document with all rector merged data");
		
		final Workbook mergedRectorsDataDoc = documentCreator.createDocument(reactors);								
		
		logger.info("Unique document with all rector merged data created");
		return mergedRectorsDataDoc;
	}
	
	private List<IAEAExtractedReactorData> importData(final FileSystem fileSystem, final Path reactorsDocumentToImportPath) throws IOException{
		
		logger.info(String.format("Importing data from path '%s'", reactorsDocumentToImportPath.toString()));
		
		final InputStream documentToImportStream = fileSystem.open(reactorsDocumentToImportPath);					
		HSSFWorkbook workbook = new HSSFWorkbook(documentToImportStream);						
	
		return extractReactorsData(workbook);			
	}

	private List<IAEAExtractedReactorData> extractReactorsData(HSSFWorkbook workbook) {
		
		final List<IAEAExtractedReactorData> reactors = new ArrayList<IAEAExtractedReactorData>();
		
		logger.info("Extracting reactors data");				
		
		for (int currentSheetNumber = 0; currentSheetNumber < workbook.getNumberOfSheets(); currentSheetNumber++){
			
			logger.debug(String.format("Extracting data from reactor %d", currentSheetNumber));
		
			final Sheet currentSheet = workbook.getSheetAt(currentSheetNumber);
			final IAEAExtractedReactorData reactorData = dataExtractor.extractFields(currentSheet);
			
			reactors.add(reactorData);														
		}												
		
		logger.info("Reactors data extraction finished");
		
		return reactors;
	}
}
