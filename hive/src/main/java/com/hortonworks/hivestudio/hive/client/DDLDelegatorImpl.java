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


package com.hortonworks.hivestudio.hive.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.dto.DumpInfo;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpInfo;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.actor.message.Connect;
import com.hortonworks.hivestudio.hive.actor.message.ExecuteJob;
import com.hortonworks.hivestudio.hive.actor.message.GetColumnMetadataJob;
import com.hortonworks.hivestudio.hive.actor.message.GetDatabaseMetadataJob;
import com.hortonworks.hivestudio.hive.actor.message.HiveJob;
import com.hortonworks.hivestudio.hive.actor.message.SQLStatementJob;
import com.hortonworks.hivestudio.hive.actor.message.job.AuthenticationFailed;
import com.hortonworks.hivestudio.hive.actor.message.job.ExecutionFailed;
import com.hortonworks.hivestudio.hive.actor.message.job.FetchFailed;
import com.hortonworks.hivestudio.hive.actor.message.job.Next;
import com.hortonworks.hivestudio.hive.actor.message.job.NoMoreItems;
import com.hortonworks.hivestudio.hive.actor.message.job.NoResult;
import com.hortonworks.hivestudio.hive.actor.message.job.Result;
import com.hortonworks.hivestudio.hive.actor.message.job.ResultSetHolder;
import com.hortonworks.hivestudio.hive.internal.dto.DatabaseInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableInfo;
import com.hortonworks.hivestudio.hive.services.HiveServiceException;
import com.hortonworks.hivestudio.hive.utils.HiveActorConfiguration;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.duration.Duration;

@Slf4j
public class DDLDelegatorImpl implements DDLDelegator {

  public static final String NO_VALUE_MARKER = "NO_VALUE";
  private final ActorRef controller;
  private final ActorSystem system;

  private final HiveContext context;
  private final HiveActorConfiguration actorConfiguration;

  public DDLDelegatorImpl(HiveContext context, Configuration configuration, ActorSystem system, ActorRef controller) {
    this.context = context;
    this.system = system;
    this.controller = controller;
    actorConfiguration = new HiveActorConfiguration(configuration);
  }

  @Override
  public void killQueries(ConnectionConfig config, String KillQueriesStatement) {
    getRowsFromDB(config, new String[]{KillQueriesStatement});
  }

  @Override
  public List<DatabaseInfo> getDbList(ConnectionConfig config, String like) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getDatabaseListStatements(like));
    List<String> databaseNames = rowsFromDB.isPresent() ? getFirstColumnValues(rowsFromDB.get().getRows()) : Lists.<String>newArrayList();
    return FluentIterable.from(databaseNames).transform(new Function<String, DatabaseInfo>() {
      public int i = 0;
      @Nullable
      @Override
      public DatabaseInfo apply(@Nullable String databaseName) {
        return new DatabaseInfo(i++, databaseName);
      }
    }).toList();
  }

  @Override
  public DumpInfo createBootstrapDumpForDB(ConnectionConfig config, String databaseName) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getBootstrapDumpStatement(databaseName),
        actorConfiguration.getReplQueryTimeout());
    return getDumpResultFromRows(databaseName, rowsFromDB);
  }

  @Override
  public DumpInfo createIncrementalDumpForDB(ConnectionConfig config, String databaseName, Integer lastReplicationId) {
    Optional<Result> rowsFromDB = getRowsFromDB(config,
        getIncrementalDumpStatement(databaseName, lastReplicationId),
        actorConfiguration.getReplQueryTimeout());
    return getDumpResultFromRows(databaseName, rowsFromDB);
  }

  private String[] getIncrementalDumpStatement(String databaseName, Integer lastReplicationId) {
    return new String[]{String.format("repl dump %s from %d with ('hive.repl.dump.metadata.only'='true', 'hive.repl.dump.include.acid.tables'='true')", databaseName, lastReplicationId)};
  }

  @Override
  public WarehouseDumpInfo createWarehouseBootstrapDump(ConnectionConfig config) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getWarehouseBootstrapDumpStatement(),
        actorConfiguration.getReplQueryTimeout());
    return getWarehouseDumpResultFromRows(rowsFromDB);
  }

  @Override
  public WarehouseDumpInfo createWarehouseIncrementalDump(ConnectionConfig config, Integer lastReplicationId, Integer maxNumberOfEvents) {
    Optional<Result> rowsFromDB = getRowsFromDB(config,
        getWarehouseIncrementalDumpStatement(lastReplicationId, maxNumberOfEvents),
        actorConfiguration.getReplQueryTimeout());
    return getWarehouseDumpResultFromRows(rowsFromDB);
  }

  private String[] getWarehouseIncrementalDumpStatement(Integer lastReplicationId, Integer maxNumberOfEvents) {
    return new String[]{String.format("repl dump `*` from %d %s with ('hive.repl.dump.metadata.only'='true', " +
        "'hive.repl.dump.include.acid.tables'='true')", lastReplicationId, (maxNumberOfEvents != null ? "LIMIT " + maxNumberOfEvents : ""))};
  }

  private String[] getBootstrapDumpStatement(String databaseName) {
    return new String[]{String.format("repl dump %s with ('hive.repl.dump.metadata.only'='true', 'hive.repl.dump.include.acid.tables'='true')", databaseName)};
  }

  private String[] getWarehouseBootstrapDumpStatement() {
    return new String[]{"repl dump `*` with ('hive.repl.dump.metadata.only'='true', 'hive.repl.dump.include.acid.tables'='true')"};
  }

  private DumpInfo getDumpResultFromRows(String databaseName, Optional<Result> rowsFromDB) {
    Object[] rowData = getFirstRowData(rowsFromDB);
    String path = (String) rowData[0];
    String lastReplicationId = (String) rowData[1];
    return new DumpInfo(databaseName, path, lastReplicationId);
  }

  private WarehouseDumpInfo getWarehouseDumpResultFromRows(Optional<Result> rowsFromDB) {
    Object[] rowData = getFirstRowData(rowsFromDB);
    String path = (String) rowData[0];
    String lastReplicationId = (String) rowData[1];
    return new WarehouseDumpInfo(path, lastReplicationId);
  }

  private Object[] getFirstRowData(Optional<Result> rowsFromDB) {
    if (rowsFromDB.isPresent()) {
      List<Row> rows = rowsFromDB.get().getRows();
      if (null != rows && rows.size() > 0) {
        Row row = rows.get(0);
        Object[] rowData = row.getRow();
        if( null != rowData && rowData.length > 0){
          return rowData;
        }else{
          throw new HiveServiceException("No columns in first row of the result");
        }
      } else {
        throw new HiveServiceException("No rows in the result");
      }
    } else {
      throw new HiveServiceException("Rows were absent the result.");
    }
  }

  @Override
  public List<TableInfo> getTableList(ConnectionConfig config, String database, String like) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getTableListStatements(database, like));
    List<String> tableNames = rowsFromDB.isPresent() ? getFirstColumnValues(rowsFromDB.get().getRows()) : Lists.<String>newArrayList();
    return FluentIterable.from(tableNames).transform(new Function<String, TableInfo>() {
      public int i = 0;
      @Nullable
      @Override
      public TableInfo apply(@Nullable String tableName) {
        return new TableInfo(i++, tableName);
      }
    }).toList();
  }

  @Override
  public List<Row> getTableDescriptionFormatted(ConnectionConfig config, String database, String table) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getTableDescriptionStatements(database, table));
    return rowsFromDB.isPresent() ? rowsFromDB.get().getRows() : null;
  }

  @Override
  public List<Row> getTableCreateStatement(ConnectionConfig config, String database, String table) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getShowCreateTableStatements(database, table));
    return rowsFromDB.isPresent() ? rowsFromDB.get().getRows() : null;
  }

  private String[] getShowCreateTableStatements(String database, String table) {
    return new String[]{
        String.format("use %s", database),
        String.format("show create table %s", table)
    };
  }

  private String[] getTableDescriptionStatements(String database, String table) {
    return new String[]{
        String.format("use %s", database),
        String.format("describe formatted %s", table)
    };
  }

  @Override
  public List<ColumnDescription> getTableDescription(ConnectionConfig config, String database, String table, String like, boolean extended) {
    Optional<Result> resultOptional = getTableDescription(config, database, table, like);
    List<ColumnDescription> descriptions = new ArrayList<>();
    if (resultOptional.isPresent()) {
      for (Row row : resultOptional.get().getRows()) {
        Object[] values = row.getRow();
        String name = (String) values[3];
        String type = (String) values[5];
        int position = (Integer) values[16];
        descriptions.add(new ColumnDescriptionShort(name, type, position));
      }
    }
    return descriptions;
  }

  @Override
  public Cursor<Row, ColumnDescription> getDbListCursor(ConnectionConfig config, String like) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getDatabaseListStatements(like));
    if (rowsFromDB.isPresent()) {
      Result result = rowsFromDB.get();
      return new PersistentCursor<>(result.getRows(), result.getColumns());
    } else {
      return new PersistentCursor<>(Lists.<Row>newArrayList(), Lists.<ColumnDescription>newArrayList());
    }
  }

  @Override
  public Cursor<Row, ColumnDescription> getTableListCursor(ConnectionConfig config, String database, String like) {
    Optional<Result> rowsFromDB = getRowsFromDB(config, getTableListStatements(database, like));
    if (rowsFromDB.isPresent()) {
      Result result = rowsFromDB.get();
      return new PersistentCursor<>(result.getRows(), result.getColumns());
    } else {
      return new PersistentCursor<>(Lists.<Row>newArrayList(), Lists.<ColumnDescription>newArrayList());
    }
  }

  @Override
  public Cursor<Row, ColumnDescription> getTableDescriptionCursor(ConnectionConfig config, String database, String table, String like, boolean extended) {
    Optional<Result> tableDescriptionOptional = getTableDescription(config, database, table, like);
    if (tableDescriptionOptional.isPresent()) {
      Result result = tableDescriptionOptional.get();
      return new PersistentCursor<>(result.getRows(), result.getColumns());
    } else {
      return new PersistentCursor<>(Lists.<Row>newArrayList(), Lists.<ColumnDescription>newArrayList());
    }
  }

  private String[] getDatabaseListStatements(String like) {
    return new String[]{
        String.format("show databases like '%s'", like)
    };
  }

  private String[] getTableListStatements(String database, String like) {
    return new String[]{
        String.format("use %s", database),
        String.format("show tables like '%s'", like)
    };
  }

  private Optional<Result> getRowsFromDB(ConnectionConfig config, String[] statements) {
    return getRowsFromDB(config, statements, actorConfiguration.getSyncQueryTimeout());
  }

  private Optional<Result> getRowsFromDB(ConnectionConfig config, String[] statements, long timeout) {
    Connect connect = config.createConnectMessage();
    HiveJob job = new SQLStatementJob(HiveJob.Type.SYNC, statements, config.getHiveContext());
    ExecuteJob execute = new ExecuteJob(connect, job);

    log.info("Executing query: {}, for user: {}", getJoinedStatements(statements), job.getHiveContext().getUsername());

    return getResultFromDB(execute, timeout);
  }

  private Optional<Result> getTableDescription(ConnectionConfig config, String databasePattern, String tablePattern, String columnPattern) {
    Connect connect = config.createConnectMessage();
    HiveJob job = new GetColumnMetadataJob(config.getHiveContext(), databasePattern, tablePattern, columnPattern);
    ExecuteJob execute = new ExecuteJob(connect, job);

    log.info("Executing query to fetch the column description for dbPattern: {}, tablePattern: {}, columnPattern: {}, for user: {}",
        databasePattern, tablePattern, columnPattern, job.getHiveContext().getUsername());
    return getResultFromDB(execute);
  }

  @Override
  public DatabaseMetadataWrapper getDatabaseMetadata(ConnectionConfig config) {
    Connect connect = config.createConnectMessage();
    HiveJob job = new GetDatabaseMetadataJob(config.getHiveContext());
    ExecuteJob execute = new ExecuteJob(connect, job);

    log.info("Fetching databaseMetadata.");
    Optional<Result> resultOptional = getResultFromDB(execute);
    if (resultOptional.isPresent()) {
      Result result = resultOptional.get();
      DatabaseMetadataWrapper databaseMetadata = result.getDatabaseMetadata();
      return databaseMetadata;
    } else {
      throw new HiveServiceException("Cannot fetch database version.");
    }
  }

  private Optional<Result> getResultFromDB(ExecuteJob job) {
    return getResultFromDB(job, actorConfiguration.getSyncQueryTimeout());
  }

  private Optional<Result> getResultFromDB(ExecuteJob job, long timeout) {
    List<ColumnDescription> descriptions = null;
    List<Row> rows = Lists.newArrayList();
    Inbox inbox = Inbox.create(system);
    inbox.send(controller, job);
    Object submitResult;
    try {
      submitResult = inbox.receive(Duration.create(timeout, TimeUnit.MILLISECONDS));
    } catch (TimeoutException ex) {
      String errorMessage = "Query timed out to fetch table description for user: " + job.getConnect().getUsername();
      log.error(errorMessage, ex);
      throw new ServiceFormattedException(errorMessage, ex);
    }

    if (submitResult instanceof NoResult) {
      log.info("Query returned with no result.");
      return Optional.absent();
    }

    if (submitResult instanceof DatabaseMetadataWrapper) {
      log.info("Query returned with no result.");
      return Optional.of(new Result((DatabaseMetadataWrapper) submitResult));
    }

    if (submitResult instanceof ExecutionFailed) {
      ExecutionFailed error = (ExecutionFailed) submitResult;
      log.error("Failed to get the table description");
      throw new ServiceFormattedException(error.getMessage(), error.getError());

    } else if (submitResult instanceof AuthenticationFailed) {
      AuthenticationFailed exception = (AuthenticationFailed) submitResult;
      log.error("Failed to connect to Hive", exception.getMessage());
      throw new ServiceFormattedException(exception.getMessage(), exception.getError(), 401);
    } else if (submitResult instanceof ResultSetHolder) {
      ResultSetHolder holder = (ResultSetHolder) submitResult;
      ActorRef iterator = holder.getIterator();
      while (true) {
        inbox.send(iterator, new Next());
        Object receive;
        try {
          receive = inbox.receive(Duration.create(actorConfiguration.getResultFetchTimeout(60 * 1000), TimeUnit.MILLISECONDS));
        } catch (TimeoutException ex) {
          String errorMessage = "Query timed out to fetch results for user: " + job.getConnect().getUsername();
          log.error(errorMessage, ex);
          throw new ServiceFormattedException(errorMessage, ex);
        }

        if (receive instanceof Result) {
          Result result = (Result) receive;
          if (descriptions == null) {
            descriptions = result.getColumns();
          }
          rows.addAll(result.getRows());
        }

        if (receive instanceof NoMoreItems) {
          break;
        }

        if (receive instanceof FetchFailed) {
          FetchFailed error = (FetchFailed) receive;
          log.error("Failed to fetch results ");
          throw new ServiceFormattedException(error.getMessage(), error.getError());
        }
      }

    }
    return Optional.of(new Result(rows, descriptions));
  }

  private String getJoinedStatements(String[] statements) {
    return Joiner.on("; ").skipNulls().join(statements);
  }

  private ImmutableList<String> getFirstColumnValues(List<Row> rows) {
    return FluentIterable.from(rows)
        .transform(new Function<Row, String>() {
          @Override
          public String apply(Row input) {
            Object[] values = input.getRow();
            return values.length > 0 ? (String) values[0] : NO_VALUE_MARKER;
          }
        }).toList();
  }

}
