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


package com.hortonworks.hivestudio.hive.internal.dto;

public class ColumnStats {
  public static final String COLUMN_NAME = "# col_name";
  public static final String DATA_TYPE = "data_type";
  public static final String MIN = "min";
  public static final String MAX = "max";
  public static final String NUM_NULLS = "num_nulls";
  public static final String DISTINCT_COUNT = "distinct_count";
  public static final String AVG_COL_LEN = "avg_col_len";
  public static final String MAX_COL_LEN = "max_col_len";
  public static final String NUM_TRUES = "num_trues";
  public static final String NUM_FALSES = "num_falses";
  public static final String COMMENT = "comment";

  private String databaseName;
  private String tableName;
  private String columnName;
  private String dataType;
  private String min;
  private String max;
  private String numNulls;
  private String distinctCount;
  private String avgColLen;
  private String maxColLen;
  private String numTrues;
  private String numFalse;
  private String comment;
  private String columnStatsAccurate;

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getMin() {
    return min;
  }

  public void setMin(String min) {
    this.min = min;
  }

  public String getMax() {
    return max;
  }

  public void setMax(String max) {
    this.max = max;
  }

  public String getNumNulls() {
    return numNulls;
  }

  public void setNumNulls(String numNulls) {
    this.numNulls = numNulls;
  }

  public String getDistinctCount() {
    return distinctCount;
  }

  public void setDistinctCount(String distinctCount) {
    this.distinctCount = distinctCount;
  }

  public String getAvgColLen() {
    return avgColLen;
  }

  public void setAvgColLen(String avgColLen) {
    this.avgColLen = avgColLen;
  }

  public String getMaxColLen() {
    return maxColLen;
  }

  public void setMaxColLen(String maxColLen) {
    this.maxColLen = maxColLen;
  }

  public String getNumTrues() {
    return numTrues;
  }

  public void setNumTrues(String numTrues) {
    this.numTrues = numTrues;
  }

  public String getNumFalse() {
    return numFalse;
  }

  public void setNumFalse(String numFalse) {
    this.numFalse = numFalse;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ColumnStats{");
    sb.append("tableName='").append(tableName).append('\'');
    sb.append(", columnName='").append(columnName).append('\'');
    sb.append(", dataType='").append(dataType).append('\'');
    sb.append(", min='").append(min).append('\'');
    sb.append(", max='").append(max).append('\'');
    sb.append(", numNulls='").append(numNulls).append('\'');
    sb.append(", distinctCount='").append(distinctCount).append('\'');
    sb.append(", avgColLen='").append(avgColLen).append('\'');
    sb.append(", maxColLen='").append(maxColLen).append('\'');
    sb.append(", numTrues='").append(numTrues).append('\'');
    sb.append(", numFalse='").append(numFalse).append('\'');
    sb.append(", comment='").append(comment).append('\'');
    sb.append('}');
    return sb.toString();
  }

  public void setColumnStatsAccurate(String columnStatsAccurate) {
    this.columnStatsAccurate = columnStatsAccurate;
  }
}
