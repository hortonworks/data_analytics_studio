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

import com.hortonworks.hivestudio.hive.client.DatabaseMetadataWrapper;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.DetailedTableInfo;
import com.hortonworks.hivestudio.hive.internal.dto.PartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.StorageInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hive.internal.dto.ViewInfo;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableStats;
import org.apache.parquet.Strings;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

/**
 *
 */
@Singleton
public class TableMetaParserImpl implements TableMetaParser<TableMeta> {

  @Inject
  private CreateTableStatementParser createTableStatementParser;

  @Inject
  private ColumnInfoParser columnInfoParser;

  @Inject
  private PartitionInfoParser partitionInfoParser;

  @Inject
  private DetailedTableInfoParser detailedTableInfoParser;

  @Inject
  private StorageInfoParser storageInfoParser;

  @Inject
  private ViewInfoParser viewInfoParser;

  @Override
  public TableMeta parse(String database, String table, List<Row> createTableStatementRows, List<Row> describeFormattedRows, DatabaseMetadataWrapper databaseMetadata) {
    String createTableStatement = createTableStatementParser.parse(createTableStatementRows);
    DetailedTableInfo tableInfo = detailedTableInfoParser.parse(describeFormattedRows);
    TableStats tableStats = getTableStats(tableInfo);
    tableStats.setDatabaseMetadata(databaseMetadata);
    StorageInfo storageInfo = storageInfoParser.parse(describeFormattedRows);
    List<ColumnInfo> columns = columnInfoParser.parse(describeFormattedRows);
    PartitionInfo partitionInfo = partitionInfoParser.parse(describeFormattedRows);
    ViewInfo viewInfo = viewInfoParser.parse(describeFormattedRows);


    TableMeta meta = new TableMeta();
    meta.setId(database + "/" + table);
    meta.setDatabase(database);
    meta.setTable(table);
    meta.setColumns(columns);
    meta.setDdl(createTableStatement);
    meta.setPartitionInfo(partitionInfo);
    meta.setDetailedInfo(tableInfo);
    meta.setStorageInfo(storageInfo);
    meta.setViewInfo(viewInfo);
    meta.setTableStats(tableStats);
    return meta;
  }

  private TableStats getTableStats(DetailedTableInfo tableInfo) {
    TableStats tableStats = new TableStats();
    tableStats.setTableStatsEnabled(false);

    String numFiles = tableInfo.getParameters().get(TableStats.NUM_FILES);
    tableInfo.getParameters().remove(TableStats.NUM_FILES);

    String numRows = tableInfo.getParameters().get(TableStats.NUM_ROWS);
    tableInfo.getParameters().remove(TableStats.NUM_ROWS);

    String columnStatsAccurate = tableInfo.getParameters().get(TableStats.COLUMN_STATS_ACCURATE);
    tableInfo.getParameters().remove(TableStats.COLUMN_STATS_ACCURATE);

    String rawDataSize = tableInfo.getParameters().get(TableStats.RAW_DATA_SIZE);
    tableInfo.getParameters().remove(TableStats.RAW_DATA_SIZE);

    String totalSize = tableInfo.getParameters().get(TableStats.TOTAL_SIZE);
    tableInfo.getParameters().remove(TableStats.TOTAL_SIZE);

    if(!Strings.isNullOrEmpty(numFiles) && !Strings.isNullOrEmpty(numFiles.trim())){
      tableStats.setTableStatsEnabled(true);
      tableStats.setNumFiles(Long.valueOf(numFiles.trim()));
    }

    if(!Strings.isNullOrEmpty(numRows) && !Strings.isNullOrEmpty(numRows.trim())){
      tableStats.setTableStatsEnabled(true);
      tableStats.setNumRows(Long.valueOf(numRows.trim()));
    }

    if(!Strings.isNullOrEmpty(rawDataSize) && !Strings.isNullOrEmpty(rawDataSize.trim())){
      tableStats.setTableStatsEnabled(true);
      tableStats.setRawDataSize(Long.valueOf(rawDataSize.trim()));
    }

    if(!Strings.isNullOrEmpty(totalSize) && !Strings.isNullOrEmpty(totalSize.trim())){
      tableStats.setTableStatsEnabled(true);
      tableStats.setTotalSize(Long.valueOf(totalSize.trim()));
    }

    if(!Strings.isNullOrEmpty(columnStatsAccurate) && !Strings.isNullOrEmpty(columnStatsAccurate.trim())) {
      tableStats.setTableStatsEnabled(true);
      tableStats.setColumnStatsAccurate(columnStatsAccurate);
    }
    return tableStats;
  }
}
