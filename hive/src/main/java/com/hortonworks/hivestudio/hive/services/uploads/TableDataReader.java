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
package com.hortonworks.hivestudio.hive.services.uploads;

import com.hortonworks.hivestudio.hive.client.ColumnDescription;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.opencsv.CSVWriter;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;

/**
 * Takes row iterator as input.
 * iterate over rows and creates a CSV formated stream separating rows by endline "\n"
 * Note : column values should not contain "\n".
 */
public class TableDataReader extends Reader {

  private static final int CAPACITY = 1024;
  private final List<ColumnInfo> header;
  private StringReader stringReader = new StringReader("");

  private Iterator<Row> iterator;
  private boolean encode = false;
  public static final char CSV_DELIMITER = '\001';

  public TableDataReader(Iterator<Row> rowIterator, List<ColumnInfo> header, boolean encode) {
    this.iterator = rowIterator;
    this.encode = encode;
    this.header = header;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {

    int totalLen = len;
    int count = 0;
    do {
      int n = stringReader.read(cbuf, off, len);

      if (n != -1) {
        // n  were read
        len = len - n; // len more to be read
        off = off + n; // off now shifted to n more
        count += n;
      }

      if (count == totalLen) return count; // all totalLen characters were read

      if (iterator.hasNext()) { // keep reading as long as we keep getting rows
        StringWriter stringWriter = new StringWriter(CAPACITY);
        CSVWriter csvPrinter = new CSVWriter(stringWriter,CSV_DELIMITER);
        Row row = iterator.next();
        // encode values so that \n and \r are overridden
        Object[] columnValues = row.getRow();
        String[] columns = new String[columnValues.length];

        for(int i = 0; i < columnValues.length; i++){
          String type = header.get(i).getType();
          if(this.encode &&
              (
                ColumnDescription.DataTypes.STRING.toString().equals(type)
                || ColumnDescription.DataTypes.VARCHAR.toString().equals(type)
                || ColumnDescription.DataTypes.CHAR.toString().equals(type)
              )
            ){
            columns[i] = Hex.encodeHexString(((String)columnValues[i]).getBytes()); //default charset
          }else {
            columns[i] = (String) columnValues[i];
          }
        }

        csvPrinter.writeNext(columns,false);
        stringReader.close(); // close the old string reader
        stringReader = new StringReader(stringWriter.getBuffer().toString());
        csvPrinter.close();
        stringWriter.close();
      } else {
        return count == 0 ? -1 : count;
      }
    } while (count < totalLen);

    return count;
  }

  @Override
  public void close() throws IOException {

  }
}
