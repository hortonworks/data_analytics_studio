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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.dto.Counter;
import com.hortonworks.hivestudio.eventProcessor.dto.CounterGroup;
import com.hortonworks.hivestudio.eventProcessor.dto.ParsedPlan;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.TableEntry;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.count.TableCount;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;
import com.hortonworks.hivestudio.hivetools.parsers.Utils;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Vertex;
import com.hortonworks.hivestudio.reporting.entities.repositories.StatsAggregator;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Daily;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Monthly;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Quarterly;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.TableStatsDBResult;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Weekly;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TSDaily;

import lombok.extern.slf4j.Slf4j;

/**
 * Class to parse the result of the query parser and find the stat related to table
 */
@Slf4j
public class TableStatsProcessor extends StatsProcessor {
  @VisibleForTesting
  Map<TableEntry, TableCount> counts = new HashMap<>();
  private final Provider<Daily> dailyTableStatsProvider;
  private final Provider<Weekly> weeklyTableStatsProvider;
  private final Provider<Monthly> monthlyTableStatsProvider;
  private final Provider<Quarterly> quarterlyTableStatsProvider;

  private final Utils queryParserUtils = new Utils();
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public TableStatsProcessor(Provider<DatabaseRepository> databaseRepositoryProvider,
                             Provider<TableRepository> tableRepositoryProvider,
                             Provider<ColumnRepository> columnRepositoryProvider,
                             Provider<Daily> dailyTableStatsProvider,
                             Provider<Weekly> weeklyTableStatsProvider,
                             Provider<Monthly> monthlyTableStatsProvider,
                             Provider<Quarterly> quarterlyTableStatsProvider) {
    super(databaseRepositoryProvider, tableRepositoryProvider, columnRepositoryProvider);
    this.dailyTableStatsProvider = dailyTableStatsProvider;
    this.weeklyTableStatsProvider = weeklyTableStatsProvider;
    this.monthlyTableStatsProvider = monthlyTableStatsProvider;
    this.quarterlyTableStatsProvider = quarterlyTableStatsProvider;
  }

  private static TableCount DUMMY_COUNT= new TableCount();
  private TableCount getCount(TableEntry entry) {
    if (entry.getDatabaseName().equals("_dummy_database")) {
      return DUMMY_COUNT;
    }
    TableCount count = counts.get(entry);
    if (count == null) {
      count = new TableCount();
      counts.put(entry, count);
    }
    return count;
  }

  @Override
  public void updateCount(ParsedPlan plan) {
    // From the query get all the distinct TableEntries
    // This will give the read count for each table
    Query query = plan.getParsedQuery();
    try {
      Set<TableEntry> uniqueTableEntries = Stream.concat(
            Stream.concat(
              queryParserUtils.extractColumns(query.getAggregations()).stream(),
              queryParserUtils.extractColumns(query.getFilters()).stream()
            ),
            queryParserUtils.extractColumns(query.getProjections()).stream())
        .map(x -> TableEntry.from(x.getTable(), plan.getQueryDate()))
        .collect(Collectors.toSet());

      for (TableEntry entry : uniqueTableEntries) {
        getCount(entry).incrementReadCount();
      }

      for (com.hortonworks.hivestudio.hivetools.parsers.entities.Table table :
        plan.getParsedQuery().getTablesWritten()) {
        TableEntry e = TableEntry.from(table, plan.getQueryDate());
        getCount(e).incrementWriteCount();
      }

      updateFromVertexStats(plan);

    } catch (Throwable t) {
      log.error("Error occurred while parsing using hive-tools.", t);
    }
  }

  private void updateFromVertexStats(ParsedPlan plan) {
    Map<String, CounterGroup> groupMap = getCounterGroupMap(plan.getCounters());
    Query query = plan.getParsedQuery();
    for (Vertex vertex : query.getVertices()) {
      com.hortonworks.hivestudio.hivetools.parsers.entities.Table table = vertex.getTable();
      if (table != null) {
        String vertexNameId = vertex.getName().replace(' ', '_');
        String counterGroupName = "TaskCounter_" + vertexNameId + "_INPUT_" + table.getAlias();
        long readRecords = getValueFromCounters(groupMap, counterGroupName, "INPUT_RECORDS_PROCESSED");
        long readBytes = getValueFromCounters(groupMap, counterGroupName, "INPUT_SPLIT_LENGTH_BYTES");
        // Add to daily table stats for table.
        TableCount count = this.getCount(TableEntry.from(table, plan.getQueryDate()));
        count.addRecordsRead(readRecords);
        count.addBytesRead(readBytes);
      }
    }
  }

  private Map<String, CounterGroup> getCounterGroupMap(ArrayNode counters) {
    Map<String, CounterGroup> map = new HashMap<>();
    if (counters == null) {
      return map;
    }
    for(JsonNode node : counters) {
      try {
        CounterGroup counterGroup = objectMapper.treeToValue(node, CounterGroup.class);
        map.put(counterGroup.getName(), counterGroup);
      } catch (JsonProcessingException e) {
        log.error("Failed to process json node for getting counters. {}", e);
      }
    }
    return map;
  }

  private static long getValueFromCounters(
      Map<String, CounterGroup> groupMap, String groupName, String name) {
    if (groupMap == null || !groupMap.containsKey(groupName)) {
      return 0L;
    }
    for (Counter counter : groupMap.get(groupName).getCounters()) {
      if (counter.getName().equalsIgnoreCase(name)) {
        return Long.parseLong(counter.getValue());
      }
    }
    return 0L;
  }

  @DASTransaction
  @Override
  public void updateCountsToDB() {
    if (counts.isEmpty()) {
      log.info("No stats to update database. Returning.");
      return;
    }

    Set<TableEntry> tableEntries = counts.keySet();
    Set<TableEntry> tableEntriesForToday = tableEntries.stream().map(TableEntry::getForToday).collect(Collectors.toSet());

    log.debug("tableEntriesForToday : {}", tableEntriesForToday);
    LocalDate minDate = tableEntries.stream().min(Comparator.comparing(TableEntry::getDate)).get().getDate();
    LocalDate maxDate = tableEntries.stream().max(Comparator.comparing(TableEntry::getDate)).get().getDate();

    log.debug("minDate : {}, maxDate : {}", minDate, maxDate);
    Map<String, Set<String>> dbToTables = tableEntries.stream()
        .collect(Collectors.groupingBy(TableEntry::getDatabaseName,
            Collectors.mapping(TableEntry::getTableName, Collectors.toSet())));

    Map<TableEntry, Table> tablesByName = getAllTablesToProcess(dbToTables);
    log.debug("tablesByName : {}", tablesByName);

    Daily dailyTableStatsRepository = dailyTableStatsProvider.get();
    Map<TableDate, TableStatsDBResult> allStatsByIdsAndDate = getAllDailyStatsByIdsAndDate(
        tableEntriesForToday, minDate, maxDate, tablesByName, dailyTableStatsRepository);
    log.debug("allStatsByIdsAndDate : {}", allStatsByIdsAndDate);

    for (TableEntry x : tableEntries) {
      TableCount tableCount = counts.get(x);
      Table tableInfo = tablesByName.get(x.getForToday());

      TableDate tableDate = new TableDate(tableInfo.getId(), x.getDate());
      if (allStatsByIdsAndDate.keySet().contains(tableDate)) {
        // Data is already present
        log.debug("Found entry : {}. Doing update.", tableDate);
        Optional<TSDaily> dailyOptional = allStatsByIdsAndDate.get(tableDate).getTableStatsDaily();
        if (dailyOptional.isPresent()) {
          TSDaily daily = dailyOptional.get();
          daily.setReadCount(daily.getReadCount() + tableCount.getReadCount());
          daily.setWriteCount(daily.getWriteCount() + tableCount.getWriteCount());
          daily.setBytesRead(daily.getBytesRead() + tableCount.getBytesRead());
          daily.setRecordsRead(daily.getRecordsRead() + tableCount.getRecordsRead());
          daily.setBytesWritten(daily.getBytesWritten() + tableCount.getBytesWritten());
          daily.setRecordsWritten(daily.getRecordsWritten() + tableCount.getRecordsWritten());
          TSDaily ts = dailyTableStatsRepository.save(daily);
          log.debug("updated TSDaily entry: {}", ts);
        }
      } else {
        // create a new entry
        log.debug("Not Found entry : {}. Doing insert.", tableDate);
        TSDaily daily = new TSDaily();
        daily.setTableId(tableInfo.getId());
        daily.setReadCount(tableCount.getReadCount());
        daily.setWriteCount(tableCount.getWriteCount());
        daily.setBytesRead(tableCount.getBytesRead());
        daily.setRecordsRead(tableCount.getRecordsRead());
        daily.setBytesWritten(tableCount.getBytesWritten());
        daily.setRecordsWritten(tableCount.getRecordsWritten());
        daily.setDate(x.getDate());
        TSDaily ts = dailyTableStatsRepository.save(daily);
        log.debug("inserted TSDaily entry: {}", ts);
      }
    }
  }

  @Override
  @DASTransaction
  public void rollupCounts(LocalDate date, SchedulerAuditType type) {
    StatsAggregator rollup;
    switch (type) {
      case WEEKLY_ROLLUP:
        rollup = weeklyTableStatsProvider.get();
        break;
      case MONTHLY_ROLLUP:
        rollup = monthlyTableStatsProvider.get();
        break;
      case QUARTERLY_ROLLUP:
        rollup = quarterlyTableStatsProvider.get();
        break;
      default:
        log.warn("Table stats count not rolled up. Unexpected SchedulerAuditType: {}", type);
        return;
    }
    log.info("Rolling up Table stats counts for type {}.", type);
    int i = rollup.rollup(date);
    log.info("Table stats counts rolled up. {} rows changed.", i);
  }

  private Map<TableDate, TableStatsDBResult> getAllDailyStatsByIdsAndDate(
      Set<TableEntry> tableEntriesForToday, LocalDate minDate, LocalDate maxDate,
      Map<TableEntry, Table> tablesByName, Daily dailyTableStatsRepository) {
    List<Integer> tableIds = tableEntriesForToday.stream()
        .map(x -> tablesByName.get(x).getId())
        .collect(Collectors.toList());

    List<TableStatsDBResult> allDailyStats = dailyTableStatsRepository.findByTablesAndTimeRange(
        tableIds, minDate, maxDate);

    return allDailyStats.stream()
        .collect(Collectors.toMap(
            x -> new TableDate(x.getTableId(), x.getDate()), Function.identity()));
  }

  private Map<TableEntry, Table> getAllTablesToProcess(Map<String, Set<String>> dbToTables) {
    Set<Table> tablesFromDB = getTablesFromDB(dbToTables);
    Map<Integer, Database> databases = new HashMap<>();
    Map<TableEntry, Table> tables = new HashMap<>();
    for (Table table : tablesFromDB) {
      Database database = databases.get(table.getDbId());
      if (database == null) {
        database = databaseRepositoryProvider.get().findOne(table.getDbId()).get();
        databases.put(table.getDbId(), database);
      }
      TableEntry entry = TableEntry.from(table, database);
      Table existing = tables.get(entry);
      if (existing == null || existing.getId() < table.getId()) {
        tables.put(entry, table);
      }
    }
    return tables;
  }
}
