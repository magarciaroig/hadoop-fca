package com.mgarciaroig.fca.etl.action.dataprepare;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

class IAEAMergedRectorsDocumentCreator {

    private final List<Field> fieldsInOrder;

    IAEAMergedRectorsDocumentCreator(List<Field> fieldsInOrder) {
        this.fieldsInOrder = fieldsInOrder;
    }

    Workbook createDocument(final Collection<IAEAExtractedReactorData> reactors) {

        final String sheetTitle = "all reactors data";

        final Workbook allReactorsWb = new HSSFWorkbook();

        final Sheet allReactorsWbSheet = allReactorsWb.createSheet(sheetTitle);

        int rownum = 0;

        rownum = createHeaderRow(allReactorsWbSheet, rownum);

        for (final IAEAExtractedReactorData currentReactor : reactors) {

            rownum = createReactorRow(allReactorsWbSheet, currentReactor, rownum);
        }

        return allReactorsWb;
    }

    private int createReactorRow(final Sheet sheet, final IAEAExtractedReactorData data, final int rownum) {

        final Row currentRow = sheet.createRow(rownum);
        final int newRownum = rownum + 1;

        int colnum = 0;

        for (final Field currentField : this.fieldsInOrder) {

            final Object fieldValue = data.retriveFieldValue(currentField);

            final Cell currentCell = currentRow.createCell(colnum++);

            fillCell(currentCell, fieldValue);
        }

        return newRownum;
    }

    private int createHeaderRow(final Sheet sheet, final int rownum) {

        final Row headerRow = sheet.createRow(rownum);

        final int newRownum = rownum + 1;

        int colnum = 0;

        for (final Field currentHeaderColumn : fieldsInOrder) {

            final Cell currentHeaderCell = headerRow.createCell(colnum++);
            currentHeaderCell.setCellValue(currentHeaderColumn.toString());
        }

        return newRownum;
    }

    private void fillCell(final Cell currentCell, final Object fieldValue) {

        if (fieldValue == null) return;

        if (fieldValue instanceof Boolean) {
            currentCell.setCellValue((Boolean) fieldValue);
        } else if (fieldValue instanceof Calendar) {
            currentCell.setCellValue((Calendar) fieldValue);
        } else if (fieldValue instanceof Date) {
            currentCell.setCellValue((Date) fieldValue);
        } else if (fieldValue instanceof Double) {
            currentCell.setCellValue((Double) fieldValue);
        } else if (fieldValue instanceof RichTextString) {
            currentCell.setCellValue((RichTextString) fieldValue);
        } else {
            currentCell.setCellValue(fieldValue.toString());
        }
    }

}
