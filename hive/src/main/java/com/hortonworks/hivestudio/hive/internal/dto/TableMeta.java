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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TableMeta implements Serializable{
  private String id;
  private String database;
  private String table;
  private List<ColumnInfo> columns;
  private String ddl;
  private PartitionInfo partitionInfo;
  private DetailedTableInfo detailedInfo;
  private TableStats tableStats;
  private StorageInfo storageInfo;
  private ViewInfo viewInfo;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<ColumnInfo> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnInfo> columns) {
    this.columns = columns;
  }

  public String getDdl() {
    return ddl;
  }

  public void setDdl(String ddl) {
    this.ddl = ddl;
  }

  public PartitionInfo getPartitionInfo() {
    return partitionInfo;
  }

  public void setPartitionInfo(PartitionInfo partitionInfo) {
    this.partitionInfo = partitionInfo;
  }

  public DetailedTableInfo getDetailedInfo() {
    return detailedInfo;
  }

  public void setDetailedInfo(DetailedTableInfo detailedInfo) {
    this.detailedInfo = detailedInfo;
  }

  public StorageInfo getStorageInfo() {
    return storageInfo;
  }

  public void setStorageInfo(StorageInfo storageInfo) {
    this.storageInfo = storageInfo;
  }

  public ViewInfo getViewInfo() {
    return viewInfo;
  }

  public void setViewInfo(ViewInfo viewInfo) {
    this.viewInfo = viewInfo;
  }

  public TableStats getTableStats() {
    return tableStats;
  }

  public void setTableStats(TableStats tableStats) {
    this.tableStats = tableStats;
  }

  public ArrayList<String> getPartitionedColumnNames() {
    ArrayList<String> partitionedColumnNames = new ArrayList<>();
    for (ColumnInfo columnInfo : getPartitionInfo().getColumns()) {
      partitionedColumnNames.add(columnInfo.getName());
    }
    return partitionedColumnNames;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TableMeta{");
    sb.append("id='").append(id).append('\'');
    sb.append(", database='").append(database).append('\'');
    sb.append(", table='").append(table).append('\'');
    sb.append(", columns=").append(columns);
    sb.append(", ddl='").append(ddl).append('\'');
    sb.append(", partitionInfo=").append(partitionInfo);
    sb.append(", detailedInfo=").append(detailedInfo);
    sb.append(", storageInfo=").append(storageInfo);
    sb.append(", viewInfo=").append(viewInfo);
    sb.append('}');
    return sb.toString();
  }
}
