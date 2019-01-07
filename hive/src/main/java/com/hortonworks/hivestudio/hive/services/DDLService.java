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

import com.hortonworks.hivestudio.common.dto.DatabaseResponse;
import com.hortonworks.hivestudio.common.dto.TableResponse;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.client.ConnectionConfig;
import com.hortonworks.hivestudio.hive.exceptions.NoQueryIdsFoundException;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnStats;
import com.hortonworks.hivestudio.hive.internal.dto.DatabaseWithTableMeta;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hive.internal.dto.TableStats;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.List;
import java.util.Set;

/**
 * Resource to get the DDL information for the database
 */
@Singleton
@Slf4j
public class DDLService {

  private static final String CREATE_TABLE = "create-table";
  private static final String ALTER_TABLE = "alter-table";
  private final DDLProxy proxy;
  private Provider<JobResourceManager> jobResourceManagerProvider = null;

  private ConnectionFactory connectionFactory;

  protected synchronized JobResourceManager getResourceManager() {
    return jobResourceManagerProvider.get();
  }

  @Inject
  public DDLService(DDLProxy proxy, Provider<JobResourceManager> jobResourceManagerProvider, ConnectionFactory connectionFactory) {
    this.proxy = proxy;
    this.jobResourceManagerProvider = jobResourceManagerProvider;
    this.connectionFactory = connectionFactory;
  }

  public Set<DatabaseResponse> getDatabases(HiveContext hiveContext, CreationSource creationSource) {
    Set<DatabaseResponse> infos = proxy.getDatabases(hiveContext, creationSource);
    return infos;
  }

  public Set<DatabaseResponse> getDatabasesFromHive(HiveContext hiveContext) {
    return proxy.getDatabasesFromHive(hiveContext);
  }

  public DatabaseResponse getDatabase(HiveContext hiveContext, String databaseName) {
    DatabaseResponse database = proxy.getDatabase(hiveContext, databaseName);
    return database;
  }

  public DatabaseResponse getDatabase(HiveContext hiveContext, Integer databaseId) {
    DatabaseResponse database = proxy.getDatabase(hiveContext, databaseId);
    return database;
  }

  public Job deleteDatabase(HiveContext hiveContext, String databaseId) {
    Job job = proxy.deleteDatabase(databaseId, getResourceManager(), hiveContext);
    return job;
  }

  public Job createDatabase(HiveContext hiveContext, String databaseId) {
    Job job = proxy.createDatabase(databaseId, getResourceManager(), hiveContext);
    return job;
  }

  public Set<TableResponse> getTables(HiveContext hiveContext, String databaseName) {
    Set<TableResponse> tables = proxy.getTables(hiveContext, databaseName);
    return tables;
  }

  public Set<TableResponse> getTables(HiveContext hiveContext, Integer databaseId) {
    Set<TableResponse> tables = proxy.getTables(hiveContext, databaseId);
    return tables;
  }

  public Job createTable(HiveContext hiveContext, String databaseName, TableMeta tableMeta) {
    Job job = proxy.createTable(databaseName, tableMeta, getResourceManager(), hiveContext);
    return job;
  }

  public Job renameTable(HiveContext hiveContext, String oldDatabaseName, String oldTableName,
                         String newDatabase, String newTable) {
    Job job = proxy.renameTable(oldDatabaseName, oldTableName, newDatabase, newTable, getResourceManager(), hiveContext);
    return job;
  }

  public Job analyzeTable(HiveContext hiveContext, String databaseName, String tableName, Boolean analyzeColumns) {
    ConnectionConfig hiveConnectionConfig = getHiveConnectionConfig(hiveContext);
    Job job = proxy.analyzeTable(databaseName, tableName, analyzeColumns, getResourceManager(), hiveConnectionConfig, hiveContext);
    return job;
  }

  public String generateDDL(HiveContext hiveContext, TableMeta tableMeta, String queryType) {
    String query = null;
    if (queryType.equals(CREATE_TABLE)) {
      query = proxy.generateCreateTableDDL(tableMeta.getDatabase(), tableMeta);
    } else if (queryType.equals(ALTER_TABLE)) {
      query = proxy.generateAlterTableQuery(hiveContext, getHiveConnectionConfig(hiveContext), tableMeta.getDatabase(),
          tableMeta.getTable(), tableMeta);
    } else {
      throw new HiveServiceException("query_type = '" + queryType + "' is not supported");
    }
    return query;
  }

  public TableResponse getTable(HiveContext hiveContext, String databaseName, String tableName) {
    TableResponse table = proxy.getTable(databaseName, tableName, hiveContext);
    return table;
  }

  public TableStats getTableStats(HiveContext hiveContext, String databaseName, String tableName) {
    TableMeta tableMeta = proxy.getTableMetaFromDescribe(databaseName, tableName, hiveContext);
    return tableMeta.getTableStats();
  }

  /**
   * @param hiveContext
   * @param databaseName
   * @param oldTableName : this is required in case if the name of table itself is changed in tableMeta
   * @param tableMeta
   * @return
   */
  public Job alterTable(HiveContext hiveContext, String databaseName, String oldTableName, TableMeta tableMeta) {
    ConnectionConfig hiveConnectionConfig = getHiveConnectionConfig(hiveContext);
    return proxy.alterTable(hiveContext, hiveConnectionConfig, databaseName, oldTableName, tableMeta, getResourceManager());
  }

  public Job deleteTable(HiveContext hiveContext, String databaseName, String tableName) {
    return proxy.deleteTable(databaseName, tableName, getResourceManager(), hiveContext);
  }

  public TableMeta getTableInfo(HiveContext hiveContext, String databaseName, String tableName) {
    TableMeta meta = proxy.getTableMetaData(databaseName, tableName); // proxy.getTableProperties(hiveContext, hiveConnectionConfig, databaseName, tableName);
    return meta;
  }

  public Job getColumnStats(HiveContext hiveContext, String databaseName, String tableName, String columnName) {
    return proxy.getColumnStatsJob(databaseName, tableName, columnName, getResourceManager(), hiveContext);
  }

  public ColumnStats fetchColumnStats(HiveContext context, String databaseName, String tablename, String columnName, Integer jobId) {
    ColumnStats columnStats = proxy.fetchColumnStats(columnName, jobId, context);
    columnStats.setTableName(tablename);
    columnStats.setDatabaseName(databaseName);
    return columnStats;
  }

  /**
   * Sends kill query request to hive for the passed queries.
   * @param queryIds query ids for which the kill has to be executed
   * @param hiveContext
   */
  public void killQueries(List<String> queryIds, HiveContext hiveContext) {
    if (queryIds.isEmpty()) {
      log.error("No query ids found. Cannot generate.");
      throw new NoQueryIdsFoundException("Null or empty query ids passed. Cannot process query string");
    }
    proxy.killQueries(queryIds, hiveContext);
  }

  protected ConnectionConfig getHiveConnectionConfig(HiveContext hiveContext) {
    return connectionFactory.create(hiveContext);
  }

  public DatabaseWithTableMeta getDatabaseWithTableMeta(HiveContext hiveContext, Integer databaseId) {
    return proxy.getDatabaseWithTableMeta(hiveContext, databaseId);
  }
}
