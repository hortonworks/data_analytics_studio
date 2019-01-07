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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.JoinEntry;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.count.JoinCount;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Join;
import com.hortonworks.hivestudio.hivetools.parsers.entities.JoinLink;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnDBResult;
import com.hortonworks.hivestudio.reporting.dto.count.JoinStatsResult;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSDaily;
import com.hortonworks.hivestudio.reporting.entities.repositories.JoinColumnStatRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.StatsAggregator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JoinStatsProcessor extends StatsProcessor {

  @VisibleForTesting
  Map<JoinEntry, JoinCount> counts = new HashMap<>();

  private Provider<JoinColumnStatRepository.Daily> dailyJoinColumnStatsRepositoryProvider;
  private Provider<JoinColumnStatRepository.Weekly> weeklyJoinColumnStatsRepositoryProvider;
  private Provider<JoinColumnStatRepository.Monthly> monthlyJoinColumnStatsRepositoryProvider;
  private Provider<JoinColumnStatRepository.Quarterly> quarterlyJoinColumnStatsRepositoryProvider;

  @Inject
  public JoinStatsProcessor(Provider<DatabaseRepository> databaseRepositoryProvider,
                            Provider<TableRepository> tableRepositoryProvider,
                            Provider<ColumnRepository> columnRepositoryProvider,
                            Provider<JoinColumnStatRepository.Daily> dailyJoinColumnStatsRepositoryProvider,
                            Provider<JoinColumnStatRepository.Weekly> weeklyJoinColumnStatsRepositoryProvider,
                            Provider<JoinColumnStatRepository.Monthly> monthlyJoinColumnStatsRepositoryProvider,
                            Provider<JoinColumnStatRepository.Quarterly> quarterlyJoinColumnStatsRepositoryProvider) {
    super(databaseRepositoryProvider, tableRepositoryProvider, columnRepositoryProvider);

    this.dailyJoinColumnStatsRepositoryProvider = dailyJoinColumnStatsRepositoryProvider;
    this.weeklyJoinColumnStatsRepositoryProvider = weeklyJoinColumnStatsRepositoryProvider;
    this.monthlyJoinColumnStatsRepositoryProvider = monthlyJoinColumnStatsRepositoryProvider;
    this.quarterlyJoinColumnStatsRepositoryProvider = quarterlyJoinColumnStatsRepositoryProvider;
  }

  private Map<JoinEntry, JoinCount> populateCount(ParsedPlan plan, Join join, JoinLink link) {
    Map<JoinEntry, JoinCount> joins = new HashMap<>();

    ColumnEntry leftColumnEntry = getColumnEntry(link.getLeftColumn());
    ColumnEntry rightColumnEntry = getColumnEntry(link.getRightColumn());
    JoinEntry joinEntry = new JoinEntry(leftColumnEntry, rightColumnEntry, join.getAlgorithmType(), plan.getQueryDate());

    JoinCount joinCount = getJoinCount(joinEntry);
    JoinCount incrementedCount = incrementCount(joinCount, join);
    joins.put(joinEntry, incrementedCount);

    return joins;
  }

  @Override
  public void updateCount(ParsedPlan plan) {
    Query query = plan.getParsedQuery();

    try {
      query.getJoins().forEach(join -> {
        JoinLink link = join.extractLink();
        if(link != null) {
          counts.putAll(populateCount(plan, join, link));
        }
        //TODO: Extract unique joins on collapsing the join list
      });
    }catch(Throwable t){
      log.error("Error occurred while parsing using hive-tools.", t);
    }
  }

  private JoinCount incrementCount(JoinCount count, Join join) {
    switch (join.getType()) {
      case INNER_JOIN:
        return count.incrementInnerJoinCount();
      case LEFT_OUTER_JOIN:
        return count.incrementLeftOuterJoinCount();
      case RIGHT_OUTER_JOIN:
        return count.incrementRightOuterJoinCount();
      case FULL_OUTER_JOIN:
        return count.incrementFullOuterJoinCount();
      case LEFT_SEMI_JOIN:
        return count.incrementLeftSemiJoinCount();
      case UNIQUE_JOIN:
        return count.incrementUniqueJoinCount();
      case UNKNOWN:
      default:
        return count.incrementUnknownJoinCount();
    }
  }

  private JoinCount getJoinCount(JoinEntry joinEntry) {
    return counts.getOrDefault(joinEntry, new JoinCount());
  }

  private ColumnEntry getColumnEntry(com.hortonworks.hivestudio.hivetools.parsers.entities.Column left) {
    return new ColumnEntry(left.getTable().getDatabaseName().toLowerCase(), left.getTable().getName().toLowerCase(), left.getColumnName().toLowerCase(), left.getColumnType());
  }

  @Override
  @DASTransaction
  public void updateCountsToDB() {
    if (counts.isEmpty()) {
      log.info("No join stats to update into database. Returning.");
      return;
    }

    Set<JoinEntry> joinEntries = counts.keySet();

    LocalDate minDate = joinEntries.stream().min(Comparator.comparing(JoinEntry::getDate)).get().getDate();
    LocalDate maxDate = joinEntries.stream().max(Comparator.comparing(JoinEntry::getDate)).get().getDate();

    Set<ColumnEntry> allColumnEntries = joinEntries.stream()
      .flatMap(x -> Stream.of(x.getLeftColumn().getForToday(), x.getRightColumn().getForToday()))
      .collect(Collectors.toSet());

    Map<String, Map<String, Set<String>>> dbTableColumns = new HashMap<>();
    for (ColumnEntry entry : allColumnEntries) {
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

    JoinColumnStatRepository.Daily dailyJoinColumnStatsRepository = dailyJoinColumnStatsRepositoryProvider.get();

    Map<JoinDate, JoinColumnDBResult> allStatsByColumnIdsAndDate = getAllDailyStatsByColumnIdsAandDate(allColumnEntries, minDate,
      maxDate, columnsByColumnEntry, dailyJoinColumnStatsRepository);


    for (JoinEntry x : joinEntries) {
      JoinCount joinCount = counts.get(x);
      Column leftColumn = columnsByColumnEntry.get(new ColumnEntry(x.getLeftColumn().getDatabaseName(), x.getLeftColumn().getTableName(), x.getLeftColumn().getColumnName(), x.getLeftColumn().getColumnType()));
      Column rightColumn = columnsByColumnEntry.get(new ColumnEntry(x.getRightColumn().getDatabaseName(), x.getRightColumn().getTableName(), x.getRightColumn().getColumnName(), x.getRightColumn().getColumnType()));

      JoinDate joinDate = new JoinDate(leftColumn.getId(), rightColumn.getId(), x.getAlgorithm().toString(), x.getDate());
      if (allStatsByColumnIdsAndDate.keySet().contains(joinDate)) {
        Optional<JCSDaily> dailyOptional = allStatsByColumnIdsAndDate.get(joinDate).getDailyOptional();
        if(dailyOptional.isPresent()) {
          JCSDaily daily = dailyOptional.get();
          daily.setInnerJoinCount(daily.getInnerJoinCount() + joinCount.getInnerJoinCount());
          daily.setLeftOuterJoinCount(daily.getLeftOuterJoinCount() + joinCount.getLeftOuterJoinCount());
          daily.setRightOuterJoinCount(daily.getRightOuterJoinCount() + joinCount.getRightOuterJoinCount());
          daily.setFullOuterJoinCount(daily.getFullOuterJoinCount() + joinCount.getFullOuterJoinCount());
          daily.setLeftSemiJoinCount(daily.getLeftSemiJoinCount() + joinCount.getLeftSemiJoinCount());
          daily.setUniqueJoinCount(daily.getUniqueJoinCount() + joinCount.getUniqueJoinCount());
          daily.setUnknownJoinCount(daily.getUnknownJoinCount() + joinCount.getUnknownJoinCount());
          daily.setTotalJoinCount(daily.getTotalJoinCount() + joinCount.getTotalJoinCount());
          dailyJoinColumnStatsRepository.save(daily);
        }
      } else {
        JCSDaily daily = new JCSDaily();
        daily.setLeftColumn(leftColumn.getId());
        daily.setRightColumn(rightColumn.getId());
        daily.setAlgorithm(x.getAlgorithm().toString());

        daily.setInnerJoinCount(joinCount.getInnerJoinCount());
        daily.setLeftOuterJoinCount(joinCount.getLeftOuterJoinCount());
        daily.setRightOuterJoinCount(joinCount.getRightOuterJoinCount());
        daily.setFullOuterJoinCount(joinCount.getFullOuterJoinCount());
        daily.setLeftSemiJoinCount(joinCount.getLeftSemiJoinCount());
        daily.setUniqueJoinCount(joinCount.getUniqueJoinCount());
        daily.setUnknownJoinCount(joinCount.getUnknownJoinCount());
        daily.setTotalJoinCount(joinCount.getTotalJoinCount());
        daily.setDate(x.getDate());
        dailyJoinColumnStatsRepository.save(daily);
      }
    }
  }

  @Override
  @DASTransaction
  public void rollupCounts(LocalDate date, SchedulerAuditType type) {
    final StatsAggregator aggregator;
    switch (type) {
      case WEEKLY_ROLLUP:
        aggregator = weeklyJoinColumnStatsRepositoryProvider.get();
        break;
      case MONTHLY_ROLLUP:
        aggregator = monthlyJoinColumnStatsRepositoryProvider.get();
        break;
      case QUARTERLY_ROLLUP:
        aggregator = quarterlyJoinColumnStatsRepositoryProvider.get();
        break;
      default:
        log.warn("Column stats count not rolled up. Unexpected SchedulerAuditType: {}", type);
        return;
    }

    log.info("Rolling up Join stats counts for type {}.", type);
    int i = aggregator.rollup(date);
    log.info("Join Column stats count rolled up. {} rows changed.", i);
  }

  private Map<JoinDate, JoinColumnDBResult> getAllDailyStatsByColumnIdsAandDate(Set<ColumnEntry> allColumnEntries, LocalDate minDate, LocalDate maxDate, Map<ColumnEntry, com.hortonworks.hivestudio.common.entities.Column> columnsByColumnEntry, JoinColumnStatRepository.Daily dailyJoinColumnStatsRepository) {
    List<Integer> columnIds = allColumnEntries.stream().map(x -> {
      Column column = columnsByColumnEntry.get(x);
      return column.getId();
    }).collect(Collectors.toList());

    List<JoinColumnDBResult> allDailyStats = dailyJoinColumnStatsRepository.findByColumnsAndTimeRange(columnIds, minDate, maxDate);
    return allDailyStats.stream().collect(
      Collectors.toMap(x -> {
        JoinStatsResult result = x.toJoinStatResult();
        return new JoinDate(result.getLeftColumnId(), result.getRightColumnId(), result.getAlgorithm(), x.getDate());
      }, Function.identity())
    );
  }

  private Map<ColumnEntry, Column> getAllColumnToProcess(
      Map<String, Map<String, Set<String>>> dbTableColumns) {
    Map<Integer, Table> tables = new HashMap<>();
    Map<Integer, Database> databases = new HashMap<>();
    Map<ColumnEntry, Column> entryColumnMap = new HashMap<>();
    for (Column column : getColumnsFromDB(dbTableColumns)) {
      Table table = tables.get(column.getTableId());
      if (table == null) {
        table = tableRepositoryProvider.get().findOne(column.getTableId()).get();
      }
      Database database = databases.get(table.getDbId());
      if (database == null) {
        database = databaseRepositoryProvider.get().findOne(table.getDbId()).get();
      }
      ColumnEntry ce = new ColumnEntry(database.getName(), table.getName(), column.getName(),
          column.getColumnType());
      Column curr = entryColumnMap.get(ce);
      if (curr == null || curr.getId() < column.getId()) {
        entryColumnMap.put(ce, column);
      }
    }
    return entryColumnMap;
  }
}
