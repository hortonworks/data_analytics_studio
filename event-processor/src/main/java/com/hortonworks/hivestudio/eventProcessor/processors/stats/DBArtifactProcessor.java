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
package com.hortonworks.hivestudio.eventProcessor.processors.stats;

import java.time.LocalDate;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.ParsedColumnType;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.common.entities.SortOrder;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.dto.ParsedPlan;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.ColumnEntry;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.TableEntry;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;
import com.hortonworks.hivestudio.hivetools.parsers.Utils;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Column;
import com.hortonworks.hivestudio.hivetools.parsers.entities.JoinLink;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;

import lombok.extern.slf4j.Slf4j;

/**
 * For each set of parsedQueries, this class keeps track of all the databases, tables and columns that are used
 * and finally inserts the artifacts which are not present at the time of execution.
 */
@Slf4j
public class DBArtifactProcessor extends StatsProcessor {

  @VisibleForTesting
  Set<String> uniqueDatabases = new HashSet<>();
  @VisibleForTesting
  Set<TableEntry> uniqueTables = new HashSet<>();
  @VisibleForTesting
  Set<ColumnEntry> uniqueColumns = new HashSet<>();

  private Utils queryParserUtils = new Utils();

  @Inject
  public DBArtifactProcessor(Provider<DatabaseRepository> databaseRepositoryProvider,
                             Provider<TableRepository> tableRepositoryProvider,
                             Provider<ColumnRepository> columnRepositoryProvider) {
    super(databaseRepositoryProvider, tableRepositoryProvider, columnRepositoryProvider);
  }

  @Override
  public void updateCount(ParsedPlan plan) {
    Set<Column> allColumnFromQuery = new HashSet<>();
    Query parsedQuery = plan.getParsedQuery();

    try {
      // Add all columns from counts
      allColumnFromQuery.addAll(queryParserUtils.extractColumns(parsedQuery.getAggregations()));
      allColumnFromQuery.addAll(queryParserUtils.extractColumns(parsedQuery.getFilters()));
      allColumnFromQuery.addAll(queryParserUtils.extractColumns(parsedQuery.getProjections()));
      allColumnFromQuery.addAll(parsedQuery.getScans());

      // Add all columns from joins
      parsedQuery.getJoins().forEach(join -> {
        JoinLink link = join.extractLink();
        if(link != null) {
          allColumnFromQuery.add(link.getLeftColumn());
          allColumnFromQuery.add(link.getRightColumn());
        }
      });
    } catch (Throwable t) {
      log.error("Error occurred while parsing using hive-tools.", t);
    }

    for (com.hortonworks.hivestudio.hivetools.parsers.entities.Table table : parsedQuery.getTablesWritten()) {
      uniqueTables.add(new TableEntry(table.getDatabaseName(), table.getName(), table.getType()));
      uniqueDatabases.add(table.getDatabaseName());
    }

    for (Column c : allColumnFromQuery) {
      String databaseName = c.getTable().getDatabaseName().toLowerCase();
      String tableName = c.getTable().getName().toLowerCase();
      String columnName = c.getColumnName().toLowerCase();
      ParsedColumnType columnType = c.getColumnType();
      uniqueDatabases.add(databaseName);
      ParsedTableType tableType = c.getTable().getType();
      TableEntry tableEntry = new TableEntry(databaseName, tableName, tableType);
      log.info("Adding table entry {} to uniqueTables.", tableEntry);
      uniqueTables.add(tableEntry);
      uniqueColumns.add(new ColumnEntry(databaseName, tableName, columnName, columnType));
    }
  }

  @DASTransaction
  @Override
  public void updateCountsToDB() {
    Set<Database> databases = updateAndGetDatabases();
    Set<Table> tables = updateAndGetTables(databases);
    updateAndGetColumns(tables);
  }

  @Override
  public void rollupCounts(LocalDate date, SchedulerAuditType type) {
    // No-op
  }

  private Set<Database> updateAndGetDatabases() {
    DatabaseRepository repository = databaseRepositoryProvider.get();
//    List<Database> allDatabaseFromDb = repository.getAllByDatabaseNames(uniqueDatabases);
    List<Database> allDatabaseFromDb = !uniqueDatabases.isEmpty() ? repository.getAllByDatabaseNames(uniqueDatabases)
        : Collections.emptyList();
    Set<String> databaseNamesFromDB = allDatabaseFromDb.stream().map(Database::getName).collect(Collectors.toSet());
    Set<String> databasesToInsert = Sets.difference(uniqueDatabases, databaseNamesFromDB);

    Set<Database> databasesInserted = databasesToInsert.stream().map(x -> {
      Database db = new Database();
      db.setName(x);
      db.setCreateTime(new Date());
      db.setDropped(Boolean.FALSE);
      db.setDroppedAt(new Date());
      db.setCreationSource(CreationSource.EVENT_PROCESSOR);
      return repository.save(db);
    }).collect(Collectors.toSet());

    if (databasesToInsert.size() > 0) {
      if (log.isDebugEnabled()) {
        log.debug("Hive database artifacts inserted to HS database. The databases are {}", databasesToInsert);
      } else {
        Set<String> dbNames = databasesInserted.stream().map(Database::getName).collect(Collectors.toSet());
        log.info("Hive database artifacts inserted to HS database. The databases are {}", dbNames);
      }
    }

    Set<Database> allDatabases = new HashSet<>();
    allDatabases.addAll(allDatabaseFromDb);
    allDatabases.addAll(databasesInserted);

    return allDatabases;
  }

  private Set<Table> updateAndGetTables(Set<Database> databases) {
    log.debug("updateAndGetTables : databases : {}", databases);
    Map<String, Set<String>> dbToTables = uniqueTables.stream()
        .collect(Collectors.groupingBy(TableEntry::getDatabaseName,
            Collectors.mapping(TableEntry::getTableName, Collectors.toSet())));

    TableRepository repository = tableRepositoryProvider.get();
    Map<String, Database> dbNameToDatabase = databases.stream().collect(
        Collectors.toMap(Database::getName, Function.identity()));

    log.debug("updateAndGetTables : dbToTables : {} ", dbToTables);
    List<Table> allTablesFromDB = Collections.emptyList();
    if (!dbToTables.isEmpty()) {
      allTablesFromDB = repository.getTableAndDatabaseByNames(dbToTables);
    }

    log.debug("allTablesFromDB : {}", allTablesFromDB);
    DatabaseRepository databaseRepository = databaseRepositoryProvider.get();
    Set<TableEntry> allTableEntriesFromDB = allTablesFromDB.stream()
        .map(x -> {
          Optional<Database> database = databaseRepository.findOne(x.getDbId());
          return new TableEntry(database.get().getName(), x.getName());
        })
        .collect(Collectors.toSet());

    log.debug("allTableEntriesFromDB : {}", allTableEntriesFromDB);
    Set<TableEntry> tablesToInsert = Sets.difference(uniqueTables, allTableEntriesFromDB);

    log.debug("tablesToInsert : {}", tablesToInsert);
    Set<Table> tablesInserted = tablesToInsert.stream().map(x -> {
      Table table = new Table();

      table.setName(x.getTableName());
      table.setCreateTime(new Date());
      table.setParsedTableType(x.getTableType());
      table.setDropped(Boolean.FALSE);
      table.setLastUpdatedAt(new Date());
      table.setDbId(dbNameToDatabase.get(x.getDatabaseName()).getId());
      table.setCreationSource(CreationSource.EVENT_PROCESSOR);

      log.debug("saving table : {}", table);
      return repository.save(table);
    }).collect(Collectors.toSet());

    if (tablesToInsert.size() > 0) {
      log.info("Hive table artifacts inserted to HS database. The tables are {}", tablesInserted);
    }

    Set<Table> allTables = new HashSet<>();
    allTables.addAll(allTablesFromDB);
    allTables.addAll(tablesInserted);
    return allTables;
  }

  private Set<com.hortonworks.hivestudio.common.entities.Column> updateAndGetColumns(Set<Table> tables) {
    ColumnRepository repository = columnRepositoryProvider.get();
    DatabaseRepository databaseRepository = databaseRepositoryProvider.get();
    Map<TableEntry, Table> tableEntryToTables = new HashMap<>();
    for (Table table : tables) {
      Optional<Database> database = databaseRepository.findOne(table.getDbId());
      if (!database.isPresent()) {
          throw new RuntimeException("Invalid database id " + table.getDbId() +
              " for table: " + table.getName() + " id: " + table.getId());
      }
      tableEntryToTables.put(new TableEntry(database.get().getName(), table.getName()), table);
    }

    Map<String, Map<String, Set<String>>> dbTableColumns = new HashMap<>();
    for (ColumnEntry entry : uniqueColumns) {
      String dbName = entry.getDatabaseName().toLowerCase();
      String tableName = entry.getTableName().toLowerCase();
      String columnName = entry.getColumnName().toLowerCase();
      Map<String, Set<String>> tableColumns = dbTableColumns.get(dbName);
      if (tableColumns == null) {
        tableColumns = new HashMap<>();
        dbTableColumns.put(dbName, tableColumns);
      }
      Set<String> columns = tableColumns.get(tableName);
      if (columns == null) {
        columns = new HashSet<>();
        tableColumns.put(tableName, columns);
      }
      columns.add(columnName);
    }

    List<com.hortonworks.hivestudio.common.entities.Column> allColumnsFromDB = Collections.emptyList();
    if (!dbTableColumns.isEmpty()) {
      allColumnsFromDB = repository.getAllByColumnAndTableAndDatabases(dbTableColumns);
    }

    TableRepository tableRepository = tableRepositoryProvider.get();
    Set<ColumnEntry> allColumnEntriesFromDB = allColumnsFromDB.stream()
        .map(x -> {
          Optional<Table> table = tableRepository.findOne(x.getTableId());
          Optional<Database> database = databaseRepository.findOne(table.get().getDbId());
          return new ColumnEntry(database.get().getName(), table.get().getName(), x.getName(), x.getColumnType());
        })
        .collect(Collectors.toSet());

    Set<ColumnEntry> columnsToInsert = Sets.difference(uniqueColumns, allColumnEntriesFromDB);

    Set<com.hortonworks.hivestudio.common.entities.Column> columnsInserted = columnsToInsert.stream()
        .map(x -> {

          com.hortonworks.hivestudio.common.entities.Column column = new com.hortonworks.hivestudio.common.entities.Column();
          column.setName(x.getColumnName());
          column.setColumnType(x.getColumnType());
          column.setCreateTime(new Date());

          column.setIsPrimary(Boolean.FALSE);
          column.setIsPartitioned(Boolean.FALSE);
          column.setIsClustered(Boolean.FALSE);
          column.setIsSortKey(Boolean.FALSE);

          column.setSortOrderEnum(SortOrder.NONE);
          column.setDropped(Boolean.FALSE);
          column.setDroppedAt(new Date());

          Table table = tableEntryToTables.get(new TableEntry(x.getDatabaseName(), x.getTableName()));
          column.setTableId(table.getId());
          column.setCreationSource(CreationSource.EVENT_PROCESSOR);

          return repository.save(column);
        }).collect(Collectors.toSet());

    if (columnsToInsert.size() > 0) {
      log.info("Hive column artifacts inserted to HS database. The columns are {}", columnsToInsert);
    }

    Set<com.hortonworks.hivestudio.common.entities.Column> allColumns = new HashSet<>();
    allColumns.addAll(allColumnsFromDB);
    allColumns.addAll(columnsInserted);

    return allColumns;
  }
}
