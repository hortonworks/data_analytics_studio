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
package com.hortonworks.hivestudio.hive.internal.generators;

import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;

import java.util.List;

public class InsertFromQueryInput {
  private String fromDatabase;
  private String fromTable;
  private String toDatabase;
  private String toTable;
  private List<ColumnInfo> partitionedColumns;
  private List<ColumnInfo> normalColumns;
  private Boolean unhexInsert = Boolean.FALSE;

  public InsertFromQueryInput() {
  }

  public InsertFromQueryInput(String fromDatabase, String fromTable, String toDatabase, String toTable,
                              List<ColumnInfo> partitionedColumns, List<ColumnInfo> normalColumns, Boolean unhexInsert) {
    this.fromDatabase = fromDatabase;
    this.fromTable = fromTable;
    this.toDatabase = toDatabase;
    this.toTable = toTable;
    this.partitionedColumns = partitionedColumns;
    this.normalColumns = normalColumns;
    this.unhexInsert = unhexInsert;
  }

  public List<ColumnInfo> getPartitionedColumns() {
    return partitionedColumns;
  }

  public void setPartitionedColumns(List<ColumnInfo> partitionedColumns) {
    this.partitionedColumns = partitionedColumns;
  }

  public List<ColumnInfo> getNormalColumns() {
    return normalColumns;
  }

  public void setNormalColumns(List<ColumnInfo> normalColumns) {
    this.normalColumns = normalColumns;
  }

  public Boolean getUnhexInsert() {
    return unhexInsert;
  }

  public void setUnhexInsert(Boolean unhexInsert) {
    this.unhexInsert = unhexInsert;
  }

  public String getFromDatabase() {
    return fromDatabase;
  }

  public void setFromDatabase(String fromDatabase) {
    this.fromDatabase = fromDatabase;
  }

  public String getFromTable() {
    return fromTable;
  }

  public void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

  public String getToDatabase() {
    return toDatabase;
  }

  public void setToDatabase(String toDatabase) {
    this.toDatabase = toDatabase;
  }

  public String getToTable() {
    return toTable;
  }

  public void setToTable(String toTable) {
    this.toTable = toTable;
  }
}
