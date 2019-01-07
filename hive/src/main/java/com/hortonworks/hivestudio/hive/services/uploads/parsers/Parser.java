/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */
package com.hortonworks.hivestudio.hive.services.uploads.parsers;

import com.hortonworks.hivestudio.hive.client.ColumnDescription;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * provides general implementation for parsing JSON,CSV,XML file
 * to generate preview rows, headers and column types
 * also provides TableDataReader for converting any type to CSV.
 */
@Slf4j
public abstract class Parser implements IParser {
  public static final String COLUMN_PREFIX = "column";

  protected Reader reader; // same as CSV reader in this case
  protected ParseOptions parseOptions;
  private int numberOfPreviewRows = 10;

  public Parser(Reader originalReader, ParseOptions parseOptions) {
    this.reader = originalReader;
    this.parseOptions = parseOptions;
  }

  /**
   * returns which datatype is valid for all the values
   */

  /**
   *
   * @param rows : non empty list of rows
   * @param colNum : to detect datatype for this column number.
   * @return data type for that column
   */
  private ColumnDescription.DataTypes getLikelyDataType(List<Row> rows, int colNum) {
    // order of detection BOOLEAN,INT,BIGINT,DOUBLE,DATE,CHAR,STRING
    List<Object> colValues = new ArrayList<>(rows.size());
    for( Row row : rows ){
      colValues.add(row.getRow()[colNum]);
    }

    return ParseUtils.detectHiveColumnDataType(colValues);
  }

  @Override
  public PreviewData parsePreview() {
    log.info("generating preview for : {}", this.parseOptions );

    ArrayList<Row> previewRows;
    List<ColumnInfo> header;

    try {
      numberOfPreviewRows = (Integer) parseOptions.getOption(ParseOptions.OPTIONS_NUMBER_OF_PREVIEW_ROWS);
    } catch (Exception e) {
      log.debug("Illegal number of preview columns supplied {}",parseOptions.getOption(ParseOptions.OPTIONS_NUMBER_OF_PREVIEW_ROWS) );
    }

    int numberOfRows = numberOfPreviewRows;
    previewRows = new ArrayList<>(numberOfPreviewRows);

    Row headerRow = null;
    Integer numOfCols = null;

    if (parseOptions.getOption(ParseOptions.OPTIONS_HEADER) != null &&
      ( parseOptions.getOption(ParseOptions.OPTIONS_HEADER).equals(ParseOptions.HEADER.FIRST_RECORD.toString()) ||
        parseOptions.getOption(ParseOptions.OPTIONS_HEADER).equals(ParseOptions.HEADER.EMBEDDED.toString())
      )) {
      headerRow = extractHeader();
      numOfCols = headerRow.getRow().length;
    }

    Row r;
    if (iterator().hasNext()) {
      r = iterator().next();
      if( null == numOfCols ) {
        numOfCols = r.getRow().length;
      }
    } else {
      log.error("No rows found in the file. returning error.");
      throw new NoSuchElementException("No rows in the file.");
    }

    while (true) {
      // create Header definition from row
      Object[] values = r.getRow();
      Object[] newValues= new Object[numOfCols]; // adds null if less columns detected and removes extra columns if any

      for (int colNum = 0; colNum < numOfCols; colNum++) {
        if(colNum < values.length) {
          newValues[colNum] = values[colNum];
        }else{
          newValues[colNum] = null;
        }
      }

      previewRows.add(new Row(newValues));

      numberOfRows--;
      if (numberOfRows <= 0 || !iterator().hasNext())
        break;

      r = iterator().next();
    }

    if (previewRows.size() <= 0) {
      log.error("No rows found in the file. returning error.");
      throw new NoSuchElementException("Does not contain any rows.");
    }

    // find data types.
    header = generateHeader(headerRow,previewRows,numOfCols);

    return new PreviewData(header, previewRows);
  }

  private List<ColumnInfo> generateHeader(Row headerRow, List<Row> previewRows, int numOfCols) {
    List<ColumnInfo> header = new ArrayList<>();

    for (int colNum = 0; colNum < numOfCols; colNum++) {
      ColumnDescription.DataTypes type = getLikelyDataType(previewRows,colNum);
      log.info("datatype detected for column {} : {}", colNum, type);

      String colName = COLUMN_PREFIX + (colNum + 1);
      if (null != headerRow)
        colName = (String) headerRow.getRow()[colNum];

      ColumnInfo cd = new ColumnInfo(colName, type.toString());
      header.add(cd);
    }

    log.debug("return headers : {} ", header);
    return header;
  }
}
