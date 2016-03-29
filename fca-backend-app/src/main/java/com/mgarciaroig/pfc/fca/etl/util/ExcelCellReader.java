package com.mgarciaroig.pfc.fca.etl.util;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;

public class ExcelCellReader {

	private final FormulaEvaluator evaluator;
	
	public ExcelCellReader(final FormulaEvaluator evaluator) {
		this.evaluator = evaluator;
	}
	
	public Object read(final Cell toBeRead) {
		
		final CellValue cellValue = evaluator.evaluate(toBeRead);
							
		Object value = null;
		
		if (cellValue != null){
			value = read(cellValue);			
		}
		
		return value;
	}
	
	private Object read(final CellValue cellValue) {
		
		final Object value;
		
		switch (cellValue.getCellType()) {
	    
			case Cell.CELL_TYPE_BOOLEAN:
		    	value = cellValue.getBooleanValue();
		        break;
	    
			case Cell.CELL_TYPE_NUMERIC:
		        value = cellValue.getNumberValue();
		        break;
	    
			case Cell.CELL_TYPE_STRING:
		        value = cellValue.getStringValue().trim();
		        break;
	        
			default:			
				value = null;	     
		}
		
		return value;	
	}

}
