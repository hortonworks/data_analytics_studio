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


package com.hortonworks.hivestudio.hive.internal.parsers;

import com.hortonworks.hivestudio.common.util.ParserUtils;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses the columns from the output of 'describe formatted ${tableName}' output
 */
@Slf4j
public class ColumnInfoParser extends AbstractTableMetaParser<List<ColumnInfo>> {
  public ColumnInfoParser() {
    super("# col_name", "", "");
  }

  @Override
  public List<ColumnInfo> parse(List<Row> rows) {
    List<ColumnInfo> columns = new ArrayList<>();
    /* General Format: Starts from the first index itself
     | # col_name                    | data_type                                                                     | comment                      |
     |                               | NULL                                                                          | NULL                         |
     | viewtime                      | int                                                                           |                              |
     | userid                        | bigint                                                                        |                              |
     | page_url                      | string                                                                        |                              |
     | referrer_url                  | string                                                                        |                              |
     | ip                            | string                                                                        | IP Address of the User       |
     |                               | NULL                                                                          | NULL                         |
     */

    /*Iterator<Row> iterator = rows.iterator();
    int index = 0;
    // Skip first two rows
    while (index < 2) {
      iterator.next();
      index++;
    }

    while (true) {
      Row row = iterator.next();
      // Columns section ends with a empty column name value
      if (index >= rows.size() || "".equalsIgnoreCase((String) row.getRow()[0]))
        break;

      String colName = (String)row.getRow()[0];
      String colType = (String)row.getRow()[1];
      String colComment = (String)row.getRow()[2];

      columns.add(new ColumnInfo(colName, colType, colComment));
      index++;
    }*/


    Map<String, Object> parsedSection = parseSection(rows);
    for(Object obj: parsedSection.values()) {
      if(obj instanceof Entry) {
        Entry entry = (Entry)obj;
        String typeInfo = entry.getValue();
        // parse precision and scale
        List<String> typePrecisionScale = ParserUtils.parseColumnDataType(typeInfo);
        String datatype = typePrecisionScale.get(0);
        String precisionString = typePrecisionScale.get(1);
        String scaleString = typePrecisionScale.get(2);
        Integer precision = !Strings.isNullOrEmpty(precisionString) ? Integer.valueOf(precisionString.trim()): null;
        Integer scale = !Strings.isNullOrEmpty(scaleString) ? Integer.valueOf(scaleString.trim()): null;
        ColumnInfo columnInfo = new ColumnInfo(entry.getName(), datatype, precision, scale, entry.getComment());
        columns.add(columnInfo);
        log.debug("found column definition : {}", columnInfo);
      }
    }
    return columns;
  }
}
