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
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.dto.ParsedPlan;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.ColumnEntry;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.count.ColumnCount;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;
import com.hortonworks.hivestudio.hivetools.parsers.Utils;
import com.hortonworks.hivestudio.hivetools.parsers.entities.JoinLink;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSDaily;
import com.hortonworks.hivestudio.reporting.entities.repositories.ColumnStatRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.StatsAggregator;

import lombok.extern.slf4j.Slf4j;

/**
 * Class to parse the result of the query parser and find the stat related to column
 */
@Slf4j
public class ColumnStatsProcessor extends StatsProcessor {

  @VisibleForTesting
  Map<ColumnEntry, ColumnCount> counts = new HashMap<>();

  private Provider<ColumnStatRepository.Daily> dailyColumnStatsProvider = null;
  private Provider<ColumnStatRepository.Weekly> weeklyColumnStatsProvider = null;
  private Provider<ColumnStatRepository.Monthly> monthlyColumnStatsProvider = null;
  private Provider<ColumnStatRepository.Quarterly> quarterlyColumnStatsProvider = null;

  private Utils queryParserUtils = new Utils();

  @Inject
  public ColumnStatsProcessor(Provider<DatabaseRepository> databaseRepositoryProvider,
                              Provider<TableRepository> tableRepositoryProvider,
                              Provider<ColumnRepository> columnRepositoryProvider,
                              Provider<ColumnStatRepository.Daily> dailyColumnStatsProvider,
                              Provider<ColumnStatRepository.Weekly> weeklyColumnStatsProvider,
                              Provider<ColumnStatRepository.Monthly> monthlyColumnStatsProvider,
                              Provider<ColumnStatRepository.Quarterly> quarterlyColumnStatsProvider
  ) {
    super(databaseRepositoryProvider, tableRepositoryProvider, columnRepositoryProvider);
    this.dailyColumnStatsProvider = dailyColumnStatsProvider;
    this.weeklyColumnStatsProvider = weeklyColumnStatsProvider;
    this.monthlyColumnStatsProvider = monthlyColumnStatsProvider;
    this.quarterlyColumnStatsProvider = quarterlyColumnStatsProvider;
  }

  @Override
  public void updateCount(ParsedPlan plan) {
    Query query = plan.getParsedQuery();

    try {
      // Get join counts
      query.getJoins().forEach(join -> {
        JoinLink link = join.extractLink();
        if(link != null) {
          ColumnEntry columnEntry = getColumnEntry(link.getLeftColumn(), plan.getQueryDate());
          ColumnCount columnCount = getColumnCount(columnEntry);
          counts.put(columnEntry, columnCount.incrementJoinCount());

          columnEntry = getColumnEntry(link.getRightColumn(), plan.getQueryDate());
          columnCount = getColumnCount(columnEntry);
          counts.put(columnEntry, columnCount.incrementJoinCount());
        }
      });

      // Get filter counts
      queryParserUtils.extractColumns(query.getFilters()).forEach(x -> {
        ColumnEntry columnEntry = getColumnEntry(x, plan.getQueryDate());
        ColumnCount columnCount = getColumnCount(columnEntry);
        counts.put(columnEntry, columnCount.incrementFilterCount());
      });

      // Get aggregation counts
      queryParserUtils.extractColumns(query.getAggregations()).forEach(x -> {
        ColumnEntry columnEntry = getColumnEntry(x, plan.getQueryDate());
        ColumnCount columnCount = getColumnCount(columnEntry);
        counts.put(columnEntry, columnCount.incrementAggregationCount());
      });

      // get projection count
      queryParserUtils.extractColumns(query.getProjections()).forEach(x -> {
        ColumnEntry columnEntry = getColumnEntry(x, plan.getQueryDate());
        ColumnCount columnCount = getColumnCount(columnEntry);
        counts.put(columnEntry, columnCount.incrementProjectionCount());
      });
    } catch (Throwable t) {
      log.error("Error occurred while parsing using hive-tools.", t);
    }
  }

  private ColumnEntry getColumnEntry(
      com.hortonworks.hivestudio.hivetools.parsers.entities.Column x, LocalDate date) {
    String databaseName = x.getTable().getDatabaseName().toLowerCase();
    String tableName = x.getTable().getName().toLowerCase();
    String columnName = x.getColumnName().toLowerCase();
    return new ColumnEntry(databaseName, tableName, columnName, x.getColumnType(), date);
  }

  private ColumnCount getColumnCount(ColumnEntry columnEntry) {
    return counts.getOrDefault(columnEntry, new ColumnCount());
  }

  @DASTransaction
  @Override
  public void updateCountsToDB() {
    if (counts.isEmpty()) {
      log.info("No column stats to update into database. Returning.");
      return;
    }

    Set<ColumnEntry> columnEntries = counts.keySet();
    Set<ColumnEntry> columnEntriesForToday = columnEntries.stream().map(ColumnEntry::getForToday).collect(Collectors.toSet());

    LocalDate minDate = null;
    LocalDate maxDate = null;
    Map<String, Map<String, Set<String>>> dbTableColumns = new HashMap<>();
    for (ColumnEntry entry : columnEntries) {
      if (minDate == null || minDate.compareTo(entry.getDate()) > 0) {
        minDate = entry.getDate();
      }
      if (maxDate == null || maxDate.compareTo(entry.getDate()) < 0) {
        maxDate = entry.getDate();
      }
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
    Map<ColumnEntry, Column> columnsByColumnEntry = getAllColumnToProcess(dbTableColumns);

    ColumnStatRepository.Daily dailyColumnStatsRepository = dailyColumnStatsProvider.get();
    Map<ColumnDate, ColumnStatRepository.ColumnStatsDBResult> allStatsByIdsAndDate =
        getAllDailyStatsByIdsAndDate(columnEntriesForToday, minDate, maxDate, columnsByColumnEntry, dailyColumnStatsRepository);

    for (ColumnEntry x : columnEntries) {
      ColumnCount columnCount = counts.get(x);
      Column columnInfo = columnsByColumnEntry.get(x.getForToday());
      ColumnDate columnDate = new ColumnDate(columnInfo.getId(), x.getDate());
      if (allStatsByIdsAndDate.containsKey(columnDate)) {
        Optional<CSDaily> dailyOptional = allStatsByIdsAndDate.get(columnDate).getCsDailyOptional();
        if (dailyOptional.isPresent()) {
          CSDaily daily = dailyOptional.get();
          daily.setFilterCount(daily.getFilterCount() + columnCount.getFilterCount());
          daily.setAggregationCount(daily.getAggregationCount() + columnCount.getAggregationCount());
          daily.setProjectionCount(daily.getProjectionCount() + columnCount.getProjectionCount());
          daily.setJoinCount(daily.getJoinCount() + columnCount.getJoinCount());
          dailyColumnStatsRepository.save(daily);
        }
      } else {
        CSDaily daily = new CSDaily();
        daily.setColumnId(columnInfo.getId());
        daily.setFilterCount(columnCount.getFilterCount());
        daily.setAggregationCount(columnCount.getAggregationCount());
        daily.setProjectionCount(columnCount.getProjectionCount());
        daily.setJoinCount(columnCount.getJoinCount());
        daily.setDate(x.getDate());
        dailyColumnStatsRepository.save(daily);
      }
    }
  }

  @Override
  @DASTransaction
  public void rollupCounts(LocalDate date, SchedulerAuditType type) {
    final StatsAggregator rollup;
    switch (type) {
      case WEEKLY_ROLLUP:
        rollup = weeklyColumnStatsProvider.get();
        break;
      case MONTHLY_ROLLUP:
        rollup = monthlyColumnStatsProvider.get();
        break;
      case QUARTERLY_ROLLUP:
        rollup = quarterlyColumnStatsProvider.get();
        break;
      default:
        log.warn("Column stats count not rolled up. Unexpected SchedulerAuditType: {}", type);
        return;
    }
    log.info("Rolling up Column Stats counts for type {}.", type.toString());
    int i = rollup.rollup(date);
    log.info("Column stats count rolled up. {} rows changed.", i);
  }

  private Map<ColumnDate, ColumnStatRepository.ColumnStatsDBResult> getAllDailyStatsByIdsAndDate(
      Set<ColumnEntry> columnEntriesForToday, LocalDate minDate, LocalDate maxDate,
      Map<ColumnEntry, Column> columnsByColumnEntry,
      ColumnStatRepository.Daily dailyColumnStatsRepository) {
    List<Integer> columnIds = columnEntriesForToday.stream()
        .map(x -> columnsByColumnEntry.get(x).getId()).collect(Collectors.toList());
    List<ColumnStatRepository.ColumnStatsDBResult> allDailyStats =
        dailyColumnStatsRepository.findByColumnsAndTimeRange(columnIds, minDate, maxDate);

    return allDailyStats
        .stream()
        .collect(
            Collectors.toMap(x -> new ColumnDate(x.getColumnId(), x.getDate())
                , Function.identity()
            )
        );
  }

  private Map<ColumnEntry, Column> getAllColumnToProcess(
      Map<String, Map<String, Set<String>>> dbTableColumns) {
    TableRepository tableRepository = tableRepositoryProvider.get();
    DatabaseRepository databaseRepository = databaseRepositoryProvider.get();
    Map<Integer, Table> tables = new HashMap<>();
    Map<Integer, Database> databases = new HashMap<>();
    Map<ColumnEntry, Column> entryColumnMap = new HashMap<>();
    for (Column column : getColumnsFromDB(dbTableColumns)) {
      Table table = tables.get(column.getTableId());
      if (table == null) {
        table = tableRepository.findOne(column.getTableId()).get();
      }
      Database database = databases.get(table.getDbId());
      if (database == null) {
        database = databaseRepository.findOne(table.getDbId()).get();
      }
      ColumnEntry ce = new ColumnEntry(database.getName(), table.getName(), column.getName(),
          column.getColumnType());
      Column curr = entryColumnMap.get(ce);
      // TODO: if a table was created and dropped and we see the same column entry
      // multiple times, we only process the last column for that interval. This can cause
      // inconsistencies. This is same in JoinStats and TableStats processor.
      if (curr == null || curr.getId() < column.getId()) {
        entryColumnMap.put(ce, column);
      }
    }
    return entryColumnMap;
  }
}
