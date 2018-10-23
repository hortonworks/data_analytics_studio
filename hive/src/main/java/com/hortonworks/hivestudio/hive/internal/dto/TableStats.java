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

import com.hortonworks.hivestudio.hive.client.DatabaseMetadataWrapper;

/**
 * this will be returned as a part of TableMeta which table info is called.
 * It includes the part of DetailedTableInfo which contain statistics related data.
 */
public class TableStats {
  public static final String NUM_FILES = "numFiles";
  public static final String NUM_ROWS = "numRows";
  public static final String COLUMN_STATS_ACCURATE = "COLUMN_STATS_ACCURATE";
  public static final String RAW_DATA_SIZE = "rawDataSize";
  public static final String TOTAL_SIZE = "totalSize";

  private DatabaseMetadataWrapper databaseMetadata;
  private Boolean isTableStatsEnabled;
  private Long numFiles;
  private Long numRows;
  private String columnStatsAccurate;
  private Long rawDataSize;
  private Long totalSize;

  public Boolean getTableStatsEnabled() {
    return isTableStatsEnabled;
  }

  public void setTableStatsEnabled(Boolean tableStatsEnabled) {
    isTableStatsEnabled = tableStatsEnabled;
  }

  public Long getNumFiles() {
    return numFiles;
  }

  public void setNumFiles(Long numFiles) {
    this.numFiles = numFiles;
  }

  public String getColumnStatsAccurate() {
    return columnStatsAccurate;
  }

  public void setColumnStatsAccurate(String columnStatsAccurate) {
    this.columnStatsAccurate = columnStatsAccurate;
  }

  public Long getRawDataSize() {
    return rawDataSize;
  }

  public void setRawDataSize(Long rawDataSize) {
    this.rawDataSize = rawDataSize;
  }

  public Long getTotalSize() {
    return totalSize;
  }

  public void setTotalSize(Long totalSize) {
    this.totalSize = totalSize;
  }

  public Long getNumRows() {
    return numRows;
  }

  public void setNumRows(Long numRows) {
    this.numRows = numRows;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TableStats{");
    sb.append("isStatsEnabled='").append(isTableStatsEnabled).append('\'');
    sb.append(", numFiles='").append(numFiles).append('\'');
    sb.append(", numRows='").append(numRows).append('\'');
    sb.append(", columnStatsAccurate='").append(columnStatsAccurate).append('\'');
    sb.append(", rawDataSize='").append(rawDataSize).append('\'');
    sb.append(", totalSize='").append(totalSize).append('\'');
    sb.append('}');
    return sb.toString();
  }

  public DatabaseMetadataWrapper getDatabaseMetadata() {
    return databaseMetadata;
  }

  public void setDatabaseMetadata(DatabaseMetadataWrapper databaseMetadata) {
    this.databaseMetadata = databaseMetadata;
  }
}
