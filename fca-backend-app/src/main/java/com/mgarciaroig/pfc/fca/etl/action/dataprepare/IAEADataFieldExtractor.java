package com.mgarciaroig.pfc.fca.etl.action.dataprepare;

import java.util.HashMap;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.transform.FieldTransformer;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.util.CellReference;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import com.mgarciaroig.pfc.fca.etl.action.dataconvert.FieldType;
import com.mgarciaroig.pfc.fca.etl.util.ExcelCellReader;

class IAEADataFieldExtractor {
	
	private final Logger logger = Logger.getLogger(this.getClass());
	
	private final HashMap<Field,String> fieldToCellMap;
	
	private final FieldTransformer transformer = new FieldTransformer();
	
	IAEADataFieldExtractor(HashMap<Field,String> fieldToCellMap){
		this.fieldToCellMap = fieldToCellMap;
	}
	
	public IAEAExtractedReactorData extractFields(final Sheet inputData){
		
		final IAEAExtractedReactorData reactorData = new IAEAExtractedReactorData();
						
		final FormulaEvaluator evaluator = inputData.getWorkbook().getCreationHelper().createFormulaEvaluator();	
		final ExcelCellReader cellReader = new ExcelCellReader(evaluator);
		
		for (final Field currentField : fieldToCellMap.keySet()){
			
			logger.info(String.format("Extracting data for field %s", currentField.toString()));
									
			final String stringfiedCellReference = fieldToCellMap.get(currentField);
			final CellReference reference = new CellReference(stringfiedCellReference);
			
			final Row row = inputData.getRow(reference.getRow());
			final Cell cell = row.getCell(reference.getCol()); 

			Object value = cellReader.read(cell);
			if (value instanceof String){
				
				value = (Object) transformer.transform(currentField.getType(), (String) value);
				
				if (currentField.getType().equals(FieldType.BOOLEAN_FIELD)){
					value = convertToBoolean(value);
					logger.debug(String.format("Converted value to boolean %b", (Boolean) value));
				}
				else if (currentField.getType().equals(FieldType.LONG_FIELD)){
					value = convertToLong(value);
					logger.debug(String.format("Converted value to long %d", (Long) value));
				}
				else if (currentField.getType().equals(FieldType.DOUBLE_FIELD)){
					value = convertToDouble(value);
					logger.debug(String.format("Converted value to double %f", (Double) value));
				}
				else if (currentField.getType().equals(FieldType.DATE_FIELD)){
					value = convertToLong(value);
					logger.debug(String.format("Converted value to date %d", (Long) value));
				}
			}												
			
			reactorData.setFieldData(currentField, value);						
		}
		
		return reactorData;			
	}	

	private Double convertToDouble(Object value) {
		
		if (valueIsUndefined(value)) return null;
		
		return Double.valueOf(value.toString());
	}

	private Long convertToLong(Object value) {
		
		if (valueIsUndefined(value)) return null;
		
		return Long.parseLong(value.toString());
	}

	private Boolean convertToBoolean(Object value) {
		
		if (valueIsUndefined(value)) return null;
		
		return value.toString().trim().toLowerCase().equals("yes") ? true : false;
	}

	private boolean valueIsUndefined(Object value) {
		return ((value == null) || (value.toString().trim().isEmpty()));
	}		
}
