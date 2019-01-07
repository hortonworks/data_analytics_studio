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
package com.hortonworks.hivestudio.hive.services;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import org.apache.parquet.Strings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.SortOrder;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnOrder;
import com.hortonworks.hivestudio.hive.internal.dto.DetailedTableInfo;
import com.hortonworks.hivestudio.hive.internal.dto.Order;
import com.hortonworks.hivestudio.hive.internal.dto.PartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.StorageInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hive.internal.dto.TableStats;

import lombok.extern.slf4j.Slf4j;

/**
 * This class provide all the live and synced objects like DB, Tables and Columns and Table stats
 */
@Slf4j
@Singleton
public class MetaStoreService {
  private final Provider<DatabaseRepository> databaseRepository;
  private final Provider<TableRepository> tableRepository;
  private final Provider<ColumnRepository> columnRepository;
  private ObjectMapper objectMapper;

  @Inject
  public MetaStoreService(Provider<DatabaseRepository> databaseRepository, Provider<TableRepository> tableRepository,
                          Provider<ColumnRepository> columnRepository, ObjectMapper objectMapper) {
    this.databaseRepository = databaseRepository;
    this.tableRepository = tableRepository;
    this.columnRepository = columnRepository;
    this.objectMapper = objectMapper;
  }

  /**
   * @return : all the alive databases
   */
  public List<Database> getDatabases() {
    return databaseRepository.get().getAllNotDropped();
  }

  public List<Database> getAllByCreationSource(CreationSource creationSource) {
    return databaseRepository.get().getAllByCreationSourceAndNotDropped(creationSource);
  }

  public Optional<Database> getDatabase(String databaseName) {
    return Optional.ofNullable(
        databaseRepository.get().getByDatabaseNameAndNotDropped(databaseName));
  }

  public Optional<Database> getDatabase(Integer databaseId) {
    Optional<Database> database = databaseRepository.get().findOne(databaseId);
    if(database.isPresent() && !database.get().getDropped()){
      return database;
    }else{
      return Optional.empty();
    }
  }

  /**
   * @param databaseId
   * @return : all the live tables in the given database
   */
  public List<Table> getTables(int databaseId) {
    return tableRepository.get().getAllForDatabaseAndNotDroppedAndSynced(databaseId);
  }

  public List<Column> getColumns(int tableId) {
    return columnRepository.get().getAllForTableNotDropped(tableId);
  }

  public Optional<Table> getTable(Integer databaseId, String tableName) {
    Table table = tableRepository.get().getByDatabaseIdAndTableNameAndNotDroppedAndSynced(databaseId, tableName);
    return Optional.ofNullable(table);
  }

  public TableMeta getTableMeta(String databaseName, String tableName) {
    Table table = tableRepository.get().getByDBNameTableNameAndNotDroppedAndSynced(databaseName, tableName);
    if (table == null) {
      log.error("No result or non-unique result found for databaseName : {} and tableName {}", databaseName, tableName);
      throw new HiveServiceException("Failed to fetch table " + databaseName + "." + tableName);
    }
    return createTableMeta(table);
  }

  public TableMeta getTableMeta(int tableId) {
    Optional<Table> table = tableRepository.get().findOne(tableId);
    if (table.isPresent()) {
      return createTableMeta(table.get());
    } else {
      throw new HiveServiceException("Table not found with id " + tableId);
    }
  }

  @VisibleForTesting
  TableMeta createTableMeta(Table table) {
    TableMeta tableMeta = new TableMeta();

    Optional<Database> database = databaseRepository.get().findOne(table.getDbId());
    tableMeta.setDatabase(database.get().getName());
    tableMeta.setTable(table.getName());

    Collection<Column> allColumns = columnRepository.get().getAllForTableNotDropped(table.getId());
    // columns not dropped
    List<Column> columns = allColumns.stream().
      filter(column -> !column.getDropped() && column.getCreationSource() == CreationSource.REPLICATION).
      collect(Collectors.toList());

    List<ColumnInfo> partitionedColumns = columns.stream()
        .filter(Column::getIsPartitioned)
        .map(MetaStoreService::transformColumnToColumnInfo)
        .collect(Collectors.toList());
    PartitionInfo partitionInfo = new PartitionInfo(partitionedColumns);
    tableMeta.setPartitionInfo(partitionInfo);

    List<ColumnInfo> normalColumns = columns.stream()
        .filter(column -> !column.getIsPartitioned())
        .map(MetaStoreService::transformColumnToColumnInfo)
        .collect(Collectors.toList());
    tableMeta.setColumns(normalColumns);

    // TODO : fetch DDL info from hive and put it in table
    tableMeta.setDdl("");
    tableMeta.setId(String.valueOf(table.getId()));

    DetailedTableInfo detailedInfo = new DetailedTableInfo();
    detailedInfo.setCreateTime(convertCreateTimeToString(table.getCreateTime()));
    detailedInfo.setDbName(database.get().getName());
    detailedInfo.setLastAccessTime(convertLastAccessTimeToString(table.getLastAccessTime()));
    detailedInfo.setLocation(table.getLocation());
    detailedInfo.setOwner(table.getOwner());
    ObjectNode properties = table.getProperties();
    @SuppressWarnings("unchecked")
    Map<String, String> parameters = objectMapper.convertValue(properties, Map.class);
    TableStats tableStats = extractTableStats(parameters);
    tableMeta.setTableStats(tableStats);
    detailedInfo.setParameters(parameters);
    detailedInfo.setRetention(String.valueOf(table.getRetention()));
    detailedInfo.setParsedTableType(table.getParsedTableType().name());;
    detailedInfo.setTableType(table.getTableType());
    detailedInfo.setTableName(table.getName());
    detailedInfo.setLastUpdatedAt(dateToString(table.getLastUpdatedAt()));

    tableMeta.setDetailedInfo(detailedInfo);

    StorageInfo storageInfo = new StorageInfo();
    List<String> bucketCols = columns.stream().filter(column -> column.getIsClustered()).map(Column::getName).collect(Collectors.toList());
    storageInfo.setBucketCols(bucketCols);
    storageInfo.setNumBuckets(String.valueOf(table.getNumBuckets()));

    storageInfo.setCompressed(String.valueOf(table.getCompressed()));
    storageInfo.setInputFormat(table.getInputFormat());
    storageInfo.setOutputFormat(table.getOutputFormat());
    storageInfo.setSerdeLibrary(table.getSerde());
    @SuppressWarnings("unchecked")
    Map<String, String> sdParams = objectMapper.convertValue(table.getStorageParameters(),
        Map.class);
    storageInfo.setParameters(sdParams);

    List<ColumnOrder> sortOrder = columns.stream()
        .filter(column -> !column.getSortOrderEnum().equals(SortOrder.NONE))
        .map(column -> new ColumnOrder(column.getName(), Order.fromOrdinal(column.getSortOrder())))
        .collect(Collectors.toList());
    storageInfo.setSortCols(sortOrder);

    tableMeta.setStorageInfo(storageInfo);

//    TODO : what is this viewInfo. Get it from hive and update here.
//    tableMeta.setViewInfo();

    return tableMeta;
  }

  private TableStats extractTableStats(Map<String, String> parameters) {
    TableStats tableStats = new TableStats();
    tableStats.setTableStatsEnabled(false);

    if(null == parameters){
      return tableStats;
    }

    String numFiles = parameters.remove(TableStats.NUM_FILES);
    String numRows = parameters.remove(TableStats.NUM_ROWS);
    String columnStatsAccurate = parameters.remove(TableStats.COLUMN_STATS_ACCURATE);
    String rawDataSize = parameters.remove(TableStats.RAW_DATA_SIZE);
    String totalSize = parameters.remove(TableStats.TOTAL_SIZE);

    if (!Strings.isNullOrEmpty(numFiles) && !Strings.isNullOrEmpty(numFiles.trim())) {
      tableStats.setTableStatsEnabled(true);
      tableStats.setNumFiles(Long.valueOf(numFiles.trim()));
    }

    if (!Strings.isNullOrEmpty(numRows) && !Strings.isNullOrEmpty(numRows.trim())) {
      tableStats.setTableStatsEnabled(true);
      tableStats.setNumRows(Long.valueOf(numRows.trim()));
    }

    if (!Strings.isNullOrEmpty(rawDataSize) && !Strings.isNullOrEmpty(rawDataSize.trim())) {
      tableStats.setTableStatsEnabled(true);
      tableStats.setRawDataSize(Long.valueOf(rawDataSize.trim()));
    }

    if (!Strings.isNullOrEmpty(totalSize) && !Strings.isNullOrEmpty(totalSize.trim())) {
      tableStats.setTableStatsEnabled(true);
      tableStats.setTotalSize(Long.valueOf(totalSize.trim()));
    }

    if (!Strings.isNullOrEmpty(columnStatsAccurate) && !Strings.isNullOrEmpty(columnStatsAccurate.trim())) {
      tableStats.setTableStatsEnabled(true);
      tableStats.setColumnStatsAccurate(columnStatsAccurate);
    }
    return tableStats;
  }

  private static ColumnInfo transformColumnToColumnInfo(Column column) {
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setComment(column.getComment());
    columnInfo.setName(column.getName());
    columnInfo.setPrecision(column.getPrecision());
    columnInfo.setScale(column.getScale());
    columnInfo.setType(column.getDatatype());

    return columnInfo;
  }

  private String convertLastAccessTimeToString(Date lastAccessTime) {
    return dateToString(lastAccessTime);
  }

  private String convertCreateTimeToString(Date createTime) {
    return dateToString(createTime);
  }

  private String dateToString(Date date) {
    // TODO : should this be transformed to some human readable format? if yes then to what format
    if (null == date) {
      return "0";
    }

    return String.valueOf(date.getTime());
  }

  public Set<TableMeta> getAllTableMetas(Integer databaseId) {
    List<Table> tables = this.getTables(databaseId);
    return tables.stream().map(table -> this.createTableMeta(table)).collect(Collectors.toSet());
  }
}
