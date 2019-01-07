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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.dto.DatabaseResponse;
import com.hortonworks.hivestudio.common.dto.DumpInfo;
import com.hortonworks.hivestudio.common.dto.TableResponse;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpInfo;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.client.ConnectionConfig;
import com.hortonworks.hivestudio.hive.client.DDLDelegator;
import com.hortonworks.hivestudio.hive.client.DDLDelegatorImpl;
import com.hortonworks.hivestudio.hive.client.DatabaseMetadataWrapper;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.exceptions.NoQueryIdsFoundException;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnStats;
import com.hortonworks.hivestudio.hive.internal.dto.DatabaseInfo;
import com.hortonworks.hivestudio.hive.internal.dto.DatabaseWithTableMeta;
import com.hortonworks.hivestudio.hive.internal.dto.TableInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hive.internal.generators.AlterTableQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.AnalyzeTableQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.CreateDatabaseQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.CreateTableQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.DeleteDatabaseQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.DeleteTableQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.FetchColumnStatsQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.KillQueriesGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.RenameTableQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.parsers.TableMetaParserImpl;
import com.hortonworks.hivestudio.hive.resources.jobs.ResultsPaginationController;
import com.hortonworks.hivestudio.hive.resources.jobs.ResultsResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class DDLProxy {
  private final TableMetaParserImpl tableMetaParser;
  private ResultsPaginationController resultsPaginationController;
  private ConnectionSystem connectionSystem;
  private Configuration configuration;
  private JobService jobService;
  private MetaStoreService metaStoreService;
  private ConnectionFactory connectionFactory;

  @Inject
  public DDLProxy(ResultsPaginationController resultsPaginationController,
                  TableMetaParserImpl tableMetaParser, ConnectionSystem connectionSystem,
                  Configuration configuration, JobService jobService,
                  MetaStoreService metaStoreService, ConnectionFactory connectionFactory) {
    log.info("Creating DDLProxy");
    this.metaStoreService = metaStoreService;
    this.connectionFactory = connectionFactory;
    this.jobService = jobService;
    this.resultsPaginationController = resultsPaginationController;
    this.connectionSystem = connectionSystem;
    this.configuration = configuration;
    this.tableMetaParser = tableMetaParser;
  }

  /**
   * returns databases but the tables inside the DatabaseResponse are always empty.
   *
   * @param hiveContext
   * @return
   */
  public Set<DatabaseResponse> getDatabases(HiveContext hiveContext) {
    Set<DatabaseInfo> infos = getDatabaseInfos(hiveContext);
    return transformToDatabasesResponse(infos);
  }

  public Set<DatabaseResponse> getDatabasesFromHive(HiveContext hiveContext) {
    Set<DatabaseInfo> infos = getDatabaseInfosFromHive(hiveContext);
    return transformToDatabasesResponse(infos);
  }

  public DatabaseResponse getDatabase(HiveContext hiveContext, final String databaseName) {
    DatabaseInfo dbInfo = selectDatabase(databaseName, hiveContext);
    List<TableInfo> tables = getTableInfos(hiveContext, dbInfo.getId());
    dbInfo.setTables(new HashSet<>(tables));

    return transformToDatabaseResponse(dbInfo);
  }

  public DatabaseResponse getDatabase(HiveContext hiveContext, final Integer databaseId) {
    DatabaseInfo dbInfo = selectDatabase(databaseId, hiveContext);
    List<TableInfo> tables = getTableInfos(hiveContext, dbInfo.getId());
    dbInfo.setTables(new HashSet<>(tables));

    return transformToDatabaseResponse(dbInfo);
  }

  public Set<TableResponse> getTables(HiveContext hiveContext, final String databaseName) {
    DatabaseInfo databaseInfo = selectDatabase(databaseName, hiveContext);

    return getTableResponses(hiveContext, databaseInfo);
  }

  public Set<TableResponse> getTables(HiveContext hiveContext, final Integer databaseId) {
    DatabaseInfo databaseInfo = selectDatabase(databaseId, hiveContext);

    return getTableResponses(hiveContext, databaseInfo);
  }

  private Set<TableResponse> getTableResponses(HiveContext hiveContext, DatabaseInfo databaseInfo) {
    List<TableInfo> tables = getTableInfos(hiveContext, databaseInfo.getId());

    return FluentIterable.from(tables).transform(new Function<TableInfo, TableResponse>() {
      @Nullable
      @Override
      public TableResponse apply(@Nullable TableInfo tableInfo) {
        TableResponse response = new TableResponse();
        response.setDatabaseId(databaseInfo.getId());
        response.setId(tableInfo.getId());
        response.setName(tableInfo.getName());
        return response;
      }
    }).toSet();
  }

  private List<TableInfo> getTableInfos(HiveContext context, Integer databaseId) {
    List<Table> tables = metaStoreService.getTables(databaseId);
    return tables.stream().map(table -> new TableInfo(table.getId(), table.getName())).collect(Collectors.toList());
  }

  public TableResponse getTable(final String databaseName, final String tableName, HiveContext hiveContext) {
    DatabaseInfo databaseInfo = selectDatabase(databaseName, hiveContext);
    TableInfo tableInfo = selectTable(hiveContext, databaseInfo.getId(), tableName);
    return transformToTableResponse(tableInfo, databaseInfo.getId());
  }

  private Optional<TableInfo> getTableInfo(HiveContext hiveContext, Integer databaseId, String tableName) {
    Optional<Table> table = metaStoreService.getTable(databaseId, tableName);
    return table.map(table1 -> new TableInfo(table1.getId(), table1.getName()));
  }

  public Job getColumnStatsJob(final String databaseName, final String tableName, final String columnName,
                               JobResourceManager resourceManager, HiveContext hiveContext) {
    FetchColumnStatsQueryGenerator queryGenerator = new FetchColumnStatsQueryGenerator(databaseName, tableName,
        columnName);
    Optional<String> q = queryGenerator.getQuery();
    String jobTitle = "Fetch column stats for " + databaseName + "." + tableName + "." + columnName;
    if (q.isPresent()) {
      String query = q.get();
      return createJob(databaseName, query, jobTitle, hiveContext);
    } else {
      throw new HiveServiceException(String.format("Failed to generate job for %s", jobTitle));
    }
  }

  /**
   * This method is deprecated instead use {@link DDLProxy#getTableMetaData(String, String)} which fetches from internal DB
   *
   * @param context
   * @param connectionConfig
   * @param databaseName
   * @param tableName
   * @return
   */
  public TableMeta getTableProperties(HiveContext context, ConnectionConfig connectionConfig, String databaseName, String tableName) {
    DDLDelegator delegator = new DDLDelegatorImpl(context, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(context));
//    List<Row> createTableStatementRows = delegator.getTableCreateStatement(connectionConfig, databaseName, tableName);
    List<Row> describeFormattedRows = delegator.getTableDescriptionFormatted(connectionConfig, databaseName, tableName);
//    DatabaseMetadataWrapper databaseMetadata = new DatabaseMetadataWrapper(1, 0, "UNKNOWN", "UNKNOWN");
//    try {
//      databaseMetadata = delegator.getDatabaseMetadata(connectionConfig);
//    } catch (HiveServiceException e) {
//      log.error("Exception while fetching hive version", e);
//    }

    return tableMetaParser.parse(databaseName, tableName, null, describeFormattedRows, null);
  }

  /**
   * fetches the latest meta data as available in hivestudio's databases.
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  public TableMeta getTableMetaData(String databaseName, String tableName) {
    return metaStoreService.getTableMeta(databaseName, tableName);
  }

  private Optional<DatabaseInfo> getDatabaseInfo(final String databaseName, HiveContext hiveContext) {
    Optional<Database> database = metaStoreService.getDatabase(databaseName);
    if (database.isPresent()) {
      return Optional.of(new DatabaseInfo(database.get().getId(), database.get().getName()));
    } else {
      return Optional.empty();
    }
  }

  private Optional<DatabaseInfo> getDatabaseInfo(final Integer databaseId, HiveContext hiveContext) {
    Optional<Database> database = metaStoreService.getDatabase(databaseId);
    if (database.isPresent()) {
      return Optional.of(new DatabaseInfo(database.get().getId(), database.get().getName()));
    } else {
      return Optional.empty();
    }
  }

  private DatabaseInfo selectDatabase(final String databaseName, HiveContext hiveContext) {
    Optional<DatabaseInfo> info = getDatabaseInfo(databaseName, hiveContext);
    if (info.isPresent()) {
      return info.get();
    } else {
      throw new ItemNotFoundException("Database Not Found with name " + databaseName);
    }
  }

  private DatabaseInfo selectDatabase(final Integer databaseId, HiveContext hiveContext) {
    Optional<DatabaseInfo> info = getDatabaseInfo(databaseId, hiveContext);
    if (info.isPresent()) {
      return info.get();
    } else {
      throw new ItemNotFoundException("Database Not Found with id " + databaseId);
    }
  }

  private Set<DatabaseResponse> transformToDatabasesResponse(Set<DatabaseInfo> infos) {
    return FluentIterable.from(infos).transform(new Function<DatabaseInfo, DatabaseResponse>() {
      @Nullable
      @Override
      public DatabaseResponse apply(@Nullable DatabaseInfo input) {
        DatabaseResponse response = new DatabaseResponse();
        response.setId(input.getId());
        response.setName(input.getName());
        return response;
      }
    }).toSet();
  }

  private DatabaseResponse transformToDatabaseResponse(DatabaseInfo databaseInfo) {
    DatabaseResponse response = new DatabaseResponse();
    response.setName(databaseInfo.getName());
    response.setId(databaseInfo.getId());
    Set<TableResponse> tableResponses = transformToTablesResponse(databaseInfo.getTables(), databaseInfo.getId());
    response.addAllTables(tableResponses);
    return response;
  }

  private Set<TableResponse> transformToTablesResponse(final Set<TableInfo> tables, final Integer databaseId) {
    return FluentIterable.from(tables).transform(new Function<TableInfo, TableResponse>() {
      @Nullable
      @Override
      public TableResponse apply(@Nullable TableInfo input) {
        return transformToTableResponse(input, databaseId);
      }
    }).toSet();
  }

  private TableResponse transformToTableResponse(TableInfo tableInfo, Integer databaseId) {
    TableResponse response = new TableResponse();
    response.setId(tableInfo.getId());
    response.setName(tableInfo.getName());
    response.setDatabaseId(databaseId);
    return response;
  }

  private TableInfo selectTable(HiveContext hiveContext, final Integer databaseId, final String tableName) {
    Optional<TableInfo> tableInfo = getTableInfo(hiveContext, databaseId, tableName);
    if(tableInfo.isPresent()){
      return tableInfo.get();
    }else{
      throw new ItemNotFoundException("Table with name " + tableName + " not found in database with id " + databaseId);
    }
  }

  public DumpInfo createBootstrapDump(HiveContext hiveContext, String databaseName) {
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(hiveContext);
    DDLDelegator delegator = new DDLDelegatorImpl(hiveContext, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(hiveContext));
    return delegator.createBootstrapDumpForDB(hiveConnectionConfig, databaseName);
  }

  public WarehouseDumpInfo createWarehouseBootstrapDump(HiveContext hiveContext) {
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(hiveContext);
    DDLDelegator delegator = new DDLDelegatorImpl(hiveContext, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(hiveContext));
    return delegator.createWarehouseBootstrapDump(hiveConnectionConfig);
  }

  private ConnectionConfig createConnectionConfig(HiveContext hiveContext) {
    return connectionFactory.create(hiveContext);
  }

  public DumpInfo createIncrementalDump(HiveContext hiveContext, String databaseName, Integer lastReplicationId) {
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(hiveContext);
    DDLDelegator delegator = new DDLDelegatorImpl(hiveContext, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(hiveContext));
    return delegator.createIncrementalDumpForDB(hiveConnectionConfig, databaseName, lastReplicationId);
  }

  public WarehouseDumpInfo createWarehouseIncrementalDump(HiveContext hiveContext, Integer lastReplicationId, Integer maxNumberOfEvents) {
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(hiveContext);
    DDLDelegator delegator = new DDLDelegatorImpl(hiveContext, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(hiveContext));
    return delegator.createWarehouseIncrementalDump(hiveConnectionConfig, lastReplicationId, maxNumberOfEvents);
  }

  private Set<DatabaseInfo> getDatabaseInfosFromHive(HiveContext hiveContext) {
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(hiveContext);
    DDLDelegator delegator = new DDLDelegatorImpl(hiveContext, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(hiveContext));
    List<DatabaseInfo> databases = delegator.getDbList(hiveConnectionConfig, "*");
    return new HashSet<>(databases);
  }

  public Set<DatabaseInfo> getDatabaseInfos(HiveContext hiveContext) {
    List<Database> databases = metaStoreService.getDatabases();
    return databases.stream().map(db -> new DatabaseInfo(db.getId(), db.getName())).collect(Collectors.toSet());
  }

  public String generateCreateTableDDL(String databaseName, TableMeta tableMeta) throws HiveServiceException {
    if (Strings.isNullOrEmpty(tableMeta.getDatabase())) {
      tableMeta.setDatabase(databaseName);
    }
    Optional<String> createTableQuery = new CreateTableQueryGenerator(tableMeta).getQuery();
    if (createTableQuery.isPresent()) {
      log.info("generated create table query : {}", createTableQuery);
      return createTableQuery.get();
    } else {
      throw new HiveServiceException("could not generate create table query for database : " + databaseName + " table : " + tableMeta.getTable());
    }
  }

  public Job createTable(String databaseName, TableMeta tableMeta, JobResourceManager resourceManager, HiveContext hiveContext) {
    String createTableQuery = this.generateCreateTableDDL(databaseName, tableMeta);
    String jobTitle = "Create table " + tableMeta.getDatabase() + "." + tableMeta.getTable();
    return createJob(databaseName, createTableQuery, jobTitle, hiveContext);
  }

  public Job deleteTable(String databaseName, String tableName, JobResourceManager resourceManager, HiveContext hiveContext) {
    String deleteTableQuery = generateDeleteTableDDL(databaseName, tableName);
    String jobTitle = "Delete table " + databaseName + "." + tableName;
    return createJob(databaseName, deleteTableQuery, jobTitle, hiveContext);
  }

  public String generateDeleteTableDDL(String databaseName, String tableName) throws HiveServiceException {
    Optional<String> deleteTableQuery = new DeleteTableQueryGenerator(databaseName, tableName).getQuery();
    if (deleteTableQuery.isPresent()) {
      log.info("deleting table {} with query {}", databaseName + "." + tableName, deleteTableQuery);
      return deleteTableQuery.get();
    } else {
      throw new HiveServiceException("Failed to generate query for delete table " + databaseName + "." + tableName);
    }
  }

  public Job alterTable(HiveContext hiveContext, ConnectionConfig hiveConnectionConfig, String databaseName, String oldTableName, TableMeta newTableMeta, JobResourceManager resourceManager) throws HiveServiceException {
    String alterQuery = generateAlterTableQuery(hiveContext, hiveConnectionConfig, databaseName, oldTableName, newTableMeta);
    String jobTitle = "Alter table " + databaseName + "." + oldTableName;
    return createJob(databaseName, alterQuery, jobTitle, hiveContext);
  }

  public TableMeta getTableMetaFromDescribe(String databaseName, String tableName, HiveContext hiveContext) {
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(hiveContext);
    return this.getTableProperties(hiveContext, hiveConnectionConfig, databaseName, tableName);
  }

  public String generateAlterTableQuery(HiveContext context, ConnectionConfig hiveConnectionConfig, String databaseName, String oldTableName, TableMeta newTableMeta) throws HiveServiceException {
    TableMeta oldTableMeta = this.getTableMetaData(databaseName, oldTableName);//this.getTableProperties(context, hiveConnectionConfig, databaseName, oldTableName);
    return generateAlterTableQuery(oldTableMeta, newTableMeta);
  }

  public String generateAlterTableQuery(TableMeta oldTableMeta, TableMeta newTableMeta) throws HiveServiceException {
    AlterTableQueryGenerator queryGenerator = new AlterTableQueryGenerator(oldTableMeta, newTableMeta);
    Optional<String> alterQuery = queryGenerator.getQuery();
    if (alterQuery.isPresent()) {
      return alterQuery.get();
    } else {
      throw new HiveServiceException("Failed to generate alter table query for table " + oldTableMeta.getDatabase() + "." + oldTableMeta.getTable() + ". No difference was found.");
    }
  }

  public Job renameTable(String oldDatabaseName, String oldTableName, String newDatabaseName, String newTableName,
                         JobResourceManager resourceManager, HiveContext hiveContext)
      throws HiveServiceException {
    RenameTableQueryGenerator queryGenerator = new RenameTableQueryGenerator(oldDatabaseName, oldTableName,
        newDatabaseName, newTableName);
    Optional<String> renameTable = queryGenerator.getQuery();
    if (renameTable.isPresent()) {
      String renameQuery = renameTable.get();
      String jobTitle = "Rename table " + oldDatabaseName + "." + oldTableName + " to " + newDatabaseName + "." +
          newTableName;
      return createJob(oldDatabaseName, renameQuery, jobTitle, hiveContext);
    } else {
      throw new HiveServiceException("Failed to generate rename table query for table " + oldDatabaseName + "." +
          oldTableName);
    }
  }

  public Job deleteDatabase(String databaseName, JobResourceManager resourceManager, HiveContext hiveContext) throws HiveServiceException {
    DeleteDatabaseQueryGenerator queryGenerator = new DeleteDatabaseQueryGenerator(databaseName);
    Optional<String> deleteDatabase = queryGenerator.getQuery();
    if (deleteDatabase.isPresent()) {
      String deleteQuery = deleteDatabase.get();
      return createJob(databaseName, deleteQuery, "Delete database " + databaseName, hiveContext);
    } else {
      throw new HiveServiceException("Failed to generate delete database query for database " + databaseName);
    }
  }

  public Job createDatabase(String databaseName, JobResourceManager resourceManager, HiveContext hiveContext) throws HiveServiceException {
    CreateDatabaseQueryGenerator queryGenerator = new CreateDatabaseQueryGenerator(databaseName);
    Optional<String> deleteDatabase = queryGenerator.getQuery();
    if (deleteDatabase.isPresent()) {
      String deleteQuery = deleteDatabase.get();
      return createJob("default", deleteQuery, "CREATE DATABASE " + databaseName, hiveContext);
    } else {
      throw new HiveServiceException("Failed to generate create database query for database " + databaseName);
    }
  }

  public Job createJob(String databaseName, String deleteQuery, String jobTitle, HiveContext hiveContext)
      throws HiveServiceException {
    log.info("Creating job for : {}", deleteQuery);
    return jobService.createJobWithUserSettings(deleteQuery, databaseName, jobTitle, Job.REFERRER.INTERNAL.name(), hiveContext);
  }

  public Job analyzeTable(String databaseName, String tableName, Boolean shouldAnalyzeColumns, JobResourceManager resourceManager, ConnectionConfig hiveConnectionConfig, HiveContext hiveContext) throws HiveServiceException {
    TableMeta tableMeta = this.getTableMetaData(databaseName, tableName);//this.getTableProperties(hiveContext, hiveConnectionConfig, databaseName, tableName);

    AnalyzeTableQueryGenerator queryGenerator = new AnalyzeTableQueryGenerator(tableMeta, shouldAnalyzeColumns);
    Optional<String> analyzeTable = queryGenerator.getQuery();
    String jobTitle = "Analyze table " + databaseName + "." + tableName;
    if (analyzeTable.isPresent()) {
      String query = analyzeTable.get();
      return createJob(databaseName, query, jobTitle, hiveContext);
    } else {
      throw new HiveServiceException("Failed to generate job for {}" + jobTitle);
    }
  }

  public ColumnStats fetchColumnStats(String columnName, Integer jobId, HiveContext context) throws HiveServiceException {
    ResultsResponse results = resultsPaginationController.getResult(jobId, null, null, null, null, context, configuration);
    if (results.getHasResults()) {
      List<String[]> rows = results.getRows();
      Map<Integer, String> headerMap = new HashMap<>();
      boolean header = true;
      ColumnStats columnStats = new ColumnStats();
      for (String[] row : rows) {
        if (header) {
          for (int i = 0; i < row.length; i++) {
            if (!Strings.isNullOrEmpty(row[i])) {
              headerMap.put(i, row[i].trim());
            }
          }
          header = false;
        } else if (row.length > 0) {
          if (columnName.equals(row[0])) { // the first column of the row contains column name
            createColumnStats(row, headerMap, columnStats);
          } else if (row.length > 1 && row[0].equalsIgnoreCase("COLUMN_STATS_ACCURATE")) {
            columnStats.setColumnStatsAccurate(row[1]);
          }
        }

        if(!StringUtils.isEmpty(row[0]) && !StringUtils.isEmpty(row[1]) && !row[1].equals("\"\"")) {
          setColumnStats(row[0], row[1], columnStats);
        }
      }

      return columnStats;
    } else {
      throw new HiveServiceException("Cannot find any result for this jobId: " + jobId);
    }
  }

  public void killQueries(List<String> queryIds, HiveContext context) {
    KillQueriesGenerator killQueriesGenerator = new KillQueriesGenerator(queryIds);
    Optional<String> queryOptional = killQueriesGenerator.getQuery();
    if (!queryOptional.isPresent()) {
      log.error("No query ids found. Cannot generate.");
      throw new NoQueryIdsFoundException("Null or empty query ids passed. Cannot process query string");
    }

    String query = queryOptional.get();
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(context);
    DDLDelegator delegator = new DDLDelegatorImpl(context, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(context));
    delegator.killQueries(hiveConnectionConfig, query);
  }

  public DatabaseMetadataWrapper getDatabaseMetaInformation(HiveContext hiveContext) {
    ConnectionConfig hiveConnectionConfig = createConnectionConfig(hiveContext);
    DDLDelegator delegator = new DDLDelegatorImpl(hiveContext, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(hiveContext));
    DatabaseMetadataWrapper databaseMetadata = delegator.getDatabaseMetadata(hiveConnectionConfig);
    return databaseMetadata;
  }

  private ColumnStats setColumnStats(String fieldName, String value, ColumnStats columnStats) {
    switch (fieldName) {
      case ColumnStats.COLUMN_NAME:
        columnStats.setColumnName(value);
        break;
      case ColumnStats.DATA_TYPE:
        columnStats.setDataType(value);
        break;
      case ColumnStats.MIN:
        columnStats.setMin(value);
        break;
      case ColumnStats.MAX:
        columnStats.setMax(value);
        break;
      case ColumnStats.NUM_NULLS:
        columnStats.setNumNulls(value);
        break;
      case ColumnStats.DISTINCT_COUNT:
        columnStats.setDistinctCount(value);
        break;
      case ColumnStats.AVG_COL_LEN:
        columnStats.setAvgColLen(value);
        break;
      case ColumnStats.MAX_COL_LEN:
        columnStats.setMaxColLen(value);
        break;
      case ColumnStats.NUM_TRUES:
        columnStats.setNumTrues(value);
        break;
      case ColumnStats.NUM_FALSES:
        columnStats.setNumFalse(value);
        break;
      case ColumnStats.COMMENT:
        columnStats.setComment(value);
    }
    return columnStats;
  }

  /**
   * order of values in array
   * row [# col_name, data_type, min, max, num_nulls, distinct_count, avg_col_len, max_col_len,num_trues,num_falses,comment]
   * indexes : 0           1        2    3     4             5               6             7           8         9    10
   *
   * @param row
   * @param headerMap
   * @param columnStats
   * @return
   */
  private ColumnStats createColumnStats(String[] row, Map<Integer, String> headerMap, ColumnStats columnStats) throws HiveServiceException {
    if (null == row) {
      throw new HiveServiceException("row cannot be null.");
    }
    for (int i = 0; i < row.length; i++) {
      setColumnStats(headerMap.get(i), row[i], columnStats);
    }

    return columnStats;
  }

  public DatabaseWithTableMeta getDatabaseWithTableMeta(HiveContext hiveContext, Integer databaseId) {
    Optional<Database> databaseOptional = metaStoreService.getDatabase(databaseId);
    if(!databaseOptional.isPresent()){
      throw new ItemNotFoundException("No live databaseOptional found for id " + databaseId);
    }

    Database database = databaseOptional.get();
    Set<TableMeta> allTableMetas = metaStoreService.getAllTableMetas(databaseId);

    return  new DatabaseWithTableMeta(database.getId(), database.getName(), allTableMetas);
  }
}