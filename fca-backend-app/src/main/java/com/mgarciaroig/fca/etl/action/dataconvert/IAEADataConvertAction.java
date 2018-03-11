package com.mgarciaroig.fca.etl.action.dataconvert;

import com.mgarciaroig.fca.etl.action.dataprepare.Field;
import com.mgarciaroig.fca.etl.util.ExcelCellReader;
import com.mgarciaroig.fca.framework.error.ReactorsFCAAnalizerException;
import com.mgarciaroig.fca.framework.oozie.BaseAction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class IAEADataConvertAction extends BaseAction {

    private static final String IMPORTED_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY = "dataImport.ImportedIAEAReactorResearchFilePathName";
    private static final String CONVERTED_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY = "dataImport.ConvertedIAEAReactorResearchFilePathName";

    public IAEADataConvertAction(String[] args) throws IOException {
        super(args);
    }

    public static void main(String[] args) throws IOException, ReactorsFCAAnalizerException {

        IAEADataConvertAction action = new IAEADataConvertAction(args);
        action.run();
    }

    @Override
    protected void run() throws ReactorsFCAAnalizerException {

        try {
            runWithoutErrorChecking();
        } catch (IOException e) {
            throw new IAEADataConvertActionException(e);
        }
    }

    private void runWithoutErrorChecking() throws IOException {

        final String etlHdfsImportedFilePathName = super.getPropertyFromConfiguration(IMPORTED_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY);
        final Path etlHdfsImportedFilePath = new Path(etlHdfsImportedFilePathName);

        final String etlHdfsConvertedFilePathName = super.getPropertyFromConfiguration(CONVERTED_REACTOR_RESEARCH_PATHNAME_CONF_PROPERTY);
        final Path etlHdfsConvertedFilePath = new Path(etlHdfsConvertedFilePathName);

        HSSFWorkbook workbook = null;

        SequenceFile.Writer writer = null;

        try {

            final FileSystem fileSystem = super.getFileSystem();

            final InputStream importedDocumentStream = fileSystem.open(etlHdfsImportedFilePath);
            workbook = new HSSFWorkbook(importedDocumentStream);

            writer = SequenceFile.createWriter(
                    super.getConfiguration(),
                    SequenceFile.Writer.file(etlHdfsConvertedFilePath),
                    SequenceFile.Writer.keyClass(NullWritable.class),
                    SequenceFile.Writer.valueClass(SortedMapWritable.class));

            final Sheet allReactorsSheet = workbook.getSheetAt(0);

            performConversion(allReactorsSheet, writer);
        } finally {
            if (workbook != null) workbook.close();

            if (writer != null) writer.close();
        }
    }

    private void performConversion(final Sheet allReactorsSheet, final SequenceFile.Writer writer) throws IOException {

        final FormulaEvaluator evaluator = allReactorsSheet.getWorkbook().getCreationHelper().createFormulaEvaluator();
        final ExcelCellReader cellReader = new ExcelCellReader(evaluator);

        final int totalRows = allReactorsSheet.getPhysicalNumberOfRows();

        ConversionSequenceFileWriter sequenceFileWriter = null;

        final int titleRowNumber = 0;

        for (int currentRowNumber = titleRowNumber; currentRowNumber < totalRows; currentRowNumber++) {

            List<Object> rowValues = loadRowValues(allReactorsSheet, currentRowNumber, cellReader);

            if (currentRowNumber == titleRowNumber) {
                final List<String> fieldNames = (List<String>) (Object) rowValues;

                sequenceFileWriter = new ConversionSequenceFileWriter(writer, fieldListFromStringfiedFields(fieldNames));
            } else {
                sequenceFileWriter.persistNewRegister(rowValues);
            }
        }
    }

    private List<Field> fieldListFromStringfiedFields(List<String> fieldCodes) {

        final List<Field> fields = new ArrayList<Field>(fieldCodes.size());

        for (final String currentFieldCode : fieldCodes) {

            fields.add(Field.buildFieldFromStringfiedRepresentation(currentFieldCode));
        }

        return fields;
    }


    private List<Object> loadRowValues(final Sheet sheet, final int rowNumber, ExcelCellReader cellReader) {

        final List<Object> values = new ArrayList<Object>();

        final Row currentRow = sheet.getRow(rowNumber);

        final int totalCols = currentRow.getPhysicalNumberOfCells();

        for (int currentColNumber = 0; currentColNumber < totalCols; currentColNumber++) {

            final Cell currentCell = currentRow.getCell(currentColNumber);

            final Object currentValue = cellReader.read(currentCell);

            values.add(currentValue);
        }

        return values;
    }
}
