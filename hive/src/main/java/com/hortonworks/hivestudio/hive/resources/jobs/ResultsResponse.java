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
package com.hortonworks.hivestudio.hive.resources.jobs;

import com.hortonworks.hivestudio.hive.client.ColumnDescription;

import java.util.ArrayList;
import java.util.List;

public class ResultsResponse {
  private List<ColumnDescription> schema;
  private List<String[]> rows;
  private int readCount;
  private boolean hasNext;
  private long offset;
  private boolean hasResults;

  public void setSchema(List<ColumnDescription> schema) {
    this.schema = schema;
  }

  public List<ColumnDescription> getSchema() {
    return schema;
  }

  public void setRows(List<Object[]> rows) {
    if (null == rows) {
      this.rows = null;
    }
    this.rows = new ArrayList<String[]>(rows.size());
    for (Object[] row : rows) {
      String[] strs = new String[row.length];
      for (int colNum = 0; colNum < row.length; colNum++) {
        String value = String.valueOf(row[colNum]);
        if (row[colNum] != null && (value.isEmpty() || value.equalsIgnoreCase("null"))) {
          strs[colNum] = String.format("\"%s\"", value);
        } else {
          strs[colNum] = value;
        }
      }
      this.rows.add(strs);
    }
  }

  public List<String[]> getRows() {
    return rows;
  }

  public void setReadCount(int readCount) {
    this.readCount = readCount;
  }

  public void setHasNext(boolean hasNext) {
    this.hasNext = hasNext;
  }

  public boolean isHasNext() {
    return hasNext;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public boolean getHasResults() {
    return hasResults;
  }

  public void setHasResults(boolean hasResults) {
    this.hasResults = hasResults;
  }
}
