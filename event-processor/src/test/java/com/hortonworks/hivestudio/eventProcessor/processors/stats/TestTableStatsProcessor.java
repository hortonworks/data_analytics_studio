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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import javax.inject.Provider;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.eventProcessor.dto.Counter;
import com.hortonworks.hivestudio.eventProcessor.dto.CounterGroup;
import com.hortonworks.hivestudio.eventProcessor.dto.ParsedPlan;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.TableEntry;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.count.TableCount;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Daily;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Monthly;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Quarterly;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository.Weekly;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TSDaily;

@RunWith(MockitoJUnitRunner.class)
public class TestTableStatsProcessor extends QueryBase {
  private TableStatsProcessor tableStatsProcessor;

  @Mock
  private Provider<DatabaseRepository> databaseRepositoryProvider;
  @Mock
  private Provider<ColumnRepository> columnRepositoryProvider;
  @Mock
  private Provider<TableRepository> tableRepositoryProvider;
  @Mock
  private Provider<Daily> dailyTableStatsProvider;
  @Mock
  private Provider<Weekly> weeklyTableStatsProvider;
  @Mock
  private Provider<Monthly> monthlyTableStatsProvider;
  @Mock
  private Provider<Quarterly> quarterlyTableStatsProvider;

  @Mock
  private DatabaseRepository databaseRepository;
  @Mock
  private ColumnRepository columnRepository;
  @Mock
  private TableRepository tableRepository;
  @Mock
  private Daily dailyTableStats;
  @Mock
  private Weekly weeklyTableStats;
  @Mock
  private Monthly monthlyTableStats;
  @Mock
  private Quarterly quarterlyTableStats;

  @Before
  public void setUp() throws Exception {
    resetQuery();
    tableStatsProcessor = new TableStatsProcessor(databaseRepositoryProvider,
        tableRepositoryProvider, columnRepositoryProvider, dailyTableStatsProvider,
        weeklyTableStatsProvider, monthlyTableStatsProvider, quarterlyTableStatsProvider);

    when(databaseRepositoryProvider.get()).thenReturn(databaseRepository);
    when(tableRepositoryProvider.get()).thenReturn(tableRepository);
//    when(columnRepositoryProvider.get()).thenReturn(columnRepository);
    when(dailyTableStatsProvider.get()).thenReturn(dailyTableStats);
    when(weeklyTableStatsProvider.get()).thenReturn(weeklyTableStats);
    when(monthlyTableStatsProvider.get()).thenReturn(monthlyTableStats);
    when(quarterlyTableStatsProvider.get()).thenReturn(quarterlyTableStats);
  }

  private CounterGroup makeCounterGroup(String groupName, String... kvPairs) {
    CounterGroup group = new CounterGroup();
    group.setName(groupName);
    List<Counter> counters = new ArrayList<>();
    for (int i = 0; i < kvPairs.length; i += 2) {
      Counter counter = new Counter();
      counter.setName(kvPairs[i]);
      counter.setValue(kvPairs[i + 1]);
      counters.add(counter);
    }
    group.setCounters(counters);
    return group;
  }

  @Test
  public void testUpdateCount() {
    ArrayNode counters = mapper.convertValue(new CounterGroup[]{
        makeCounterGroup("TaskCounter_Map_1_INPUT_catalog_sales", "INPUT_RECORDS_PROCESSED", "1",
            "INPUT_SPLIT_LENGTH_BYTES", "10"),
        makeCounterGroup("TaskCounter_Map_2_INPUT_cd1", "INPUT_RECORDS_PROCESSED", "2",
            "INPUT_SPLIT_LENGTH_BYTES", "20"),
        makeCounterGroup("TaskCounter_Map_3_INPUT_date_dim", "INPUT_RECORDS_PROCESSED", "3",
            "INPUT_SPLIT_LENGTH_BYTES", "30"),
        makeCounterGroup("TaskCounter_Map_4_INPUT_item", "INPUT_RECORDS_PROCESSED", "4",
            "INPUT_SPLIT_LENGTH_BYTES", "40"),
        makeCounterGroup("TaskCounter_Map_5_INPUT_customer_address", "INPUT_RECORDS_PROCESSED", "5",
            "INPUT_SPLIT_LENGTH_BYTES", "50"),
        makeCounterGroup("TaskCounter_Map_6_INPUT_customer", "INPUT_RECORDS_PROCESSED", "6",
            "INPUT_SPLIT_LENGTH_BYTES", "60"),
        makeCounterGroup("TaskCounter_Map_7_INPUT_cd2", "INPUT_RECORDS_PROCESSED", "7",
            "INPUT_SPLIT_LENGTH_BYTES", "70"),
    }, ArrayNode.class);
    ParsedPlan parsedPlan = new ParsedPlan(query, counters, LocalDate.now());
    tableStatsProcessor.updateCount(parsedPlan);

    for (Entry<TableEntry, TableCount> entry : tableStatsProcessor.counts.entrySet()) {
      long rec = 0;
      String tableName = entry.getKey().getTableName();
      TableCount count = entry.getValue();
      switch (tableName) {
        case "catalog_sales":
          rec = 1;
          break;
        case "customer_demographics":
          rec = 2 + 7;
          break;
        case "date_dim":
          rec = 3;
          break;
        case "item":
          rec = 4;
          break;
        case "customer_address":
          rec = 5;
          break;
        case "customer":
          rec = 6;
          break;
        default:
          Assert.fail("Unexpected table name: " + tableName);
      }
      Assert.assertEquals("Invalid count for table: " + tableName, rec, count.getRecordsRead());
      Assert.assertEquals("Invalid count for table: " + tableName, rec * 10, count.getBytesRead());
      Assert.assertEquals("Invalid count for table: " + tableName, 0, count.getRecordsWritten());
      Assert.assertEquals("Invalid count for table: " + tableName, 0, count.getBytesWritten());
      Assert.assertEquals("Invalid count for table: " + tableName, 1, count.getReadCount());
      Assert.assertEquals("Invalid count for table: " + tableName, 0, count.getWriteCount());
    }
    Assert.assertEquals("Invalid table count", 6, tableStatsProcessor.counts.size());
  }

  @Test
  public void testRollup() throws Exception {
    LocalDate date = LocalDate.now();

    Weekly weeklyMock = mock(Weekly.class);
    when(weeklyTableStatsProvider.get()).thenReturn(weeklyMock);
    when(weeklyMock.rollup(date)).thenReturn(1);
    tableStatsProcessor.rollupCounts(date, SchedulerAuditType.WEEKLY_ROLLUP);
    verify(weeklyMock).rollup(date);

    Monthly monthlyMock = mock(Monthly.class);
    when(monthlyTableStatsProvider.get()).thenReturn(monthlyMock);
    when(monthlyMock.rollup(date)).thenReturn(2);
    tableStatsProcessor.rollupCounts(date, SchedulerAuditType.MONTHLY_ROLLUP);
    verify(monthlyMock).rollup(date);

    Quarterly quarterlyMock = mock(Quarterly.class);
    when(quarterlyTableStatsProvider.get()).thenReturn(quarterlyMock);
    when(quarterlyMock.rollup(date)).thenReturn(3);
    tableStatsProcessor.rollupCounts(date, SchedulerAuditType.QUARTERLY_ROLLUP);
    verify(quarterlyMock).rollup(date);
  }

  @Test
  public void updateCountsToDB_Empty() {
    Map<TableEntry, TableCount> counts = new HashMap<>();
    tableStatsProcessor.counts = counts;

    tableStatsProcessor.updateCountsToDB();

    verifyZeroInteractions(databaseRepositoryProvider,
        columnRepositoryProvider,
        tableRepositoryProvider,
        dailyTableStatsProvider,
        weeklyTableStatsProvider,
        monthlyTableStatsProvider,
        quarterlyTableStatsProvider);
  }

  @Test
  public void updateCountsToDB() {
//    setups
    Map<TableEntry, TableCount> counts = new HashMap<>();
    Integer databaseId1 = 1;
    String database1 = "databaseName1";
    String table1 = "table1";
    LocalDate date = LocalDate.now();

    Integer databaseId2 = 2;
    String database2 = "databaseName2";
    String table2 = "table2";

    Integer databaseId3 = 3;
    String database3 = "databaseName3";
    String table3 = "table3";

    TableEntry te1 = new TableEntry(database1, table1, date, ParsedTableType.NORMAL);
    TableEntry te2 = new TableEntry(database2, table2, date, ParsedTableType.NORMAL);
    TableEntry te3 = new TableEntry(database3, table3, date, ParsedTableType.NORMAL);
    TableEntry te4 = new TableEntry(database3, table3, date, ParsedTableType.NORMAL);

    Database db1 = new Database();
    db1.setId(databaseId1);
    db1.setName(database1);
    Table t1 = new Table();
    t1.setId(1);
    t1.setName(table1);
    t1.setDbId(db1.getId());

    Database db2 = new Database();
    db2.setId(databaseId2);
    db2.setName(database2);
    Table t2 = new Table();
    t2.setId(2);
    t2.setName(table2);
    t2.setDbId(db2.getId());

    Database db3 = new Database();
    db3.setId(databaseId3);
    db3.setName(database3);
    Table t3 = new Table();
    t3.setId(3);
    t3.setName(table3);
    t3.setDbId(db3.getId());

    // Same as table3 but higher id.
    Table t4 = new Table();
    t4.setId(4);
    t4.setName(table3);
    t4.setDbId(db3.getId());


    TableCount tc1 = TableCount.builder().bytesRead(1).bytesWritten(1).readCount(1).recordsRead(1).recordsWritten(1).writeCount(1).build();
    TableCount tc2 = TableCount.builder().bytesRead(2).bytesWritten(2).readCount(2).recordsRead(2).recordsWritten(2).writeCount(2).build();
    TableCount tc3 = TableCount.builder().bytesRead(3).bytesWritten(3).readCount(3).recordsRead(3).recordsWritten(3).writeCount(3).build();
    TableCount tc4 = TableCount.builder().bytesRead(4).bytesWritten(4).readCount(4).recordsRead(4).recordsWritten(4).writeCount(4).build();

    counts.put(te1, tc1);
    counts.put(te2, tc2);
    counts.put(te3, tc3);
    counts.put(te4, tc4);

    TSDaily tsdaily1 = TSDaily.builder().id(10).date(date)
        .readCount(1)
        .writeCount(1)
        .bytesRead(1l)
        .bytesWritten(1l)
        .recordsRead(1l)
        .recordsWritten(1l)
        .tableId(t1.getId())
        .build();

    TSDaily tsdaily2 = TSDaily.builder().id(20).date(date)
        .readCount(1)
        .writeCount(1)
        .bytesRead(1l)
        .bytesWritten(1l)
        .recordsRead(1l)
        .recordsWritten(1l)
        .tableId(t2.getId())
        .build();

    TableStatRepository.TableStatsDBResult result1 = new TableStatRepository.TableStatsDBResult(tsdaily1);
    TableStatRepository.TableStatsDBResult result2 = new TableStatRepository.TableStatsDBResult(tsdaily2);

// mocks
    when(tableRepository.getTableAndDatabaseByNames(any())).thenReturn(Arrays.asList(t1, t2, t3, t4));
    when(dailyTableStats.findByTablesAndTimeRange(any(), any(), any())).thenReturn(Arrays.asList(result1, result2));
    tableStatsProcessor.counts = counts;

    when(databaseRepository.findOne(1)).thenReturn(Optional.of(db1));
    when(databaseRepository.findOne(2)).thenReturn(Optional.of(db2));
    when(databaseRepository.findOne(3)).thenReturn(Optional.of(db3));

//    method call
    tableStatsProcessor.updateCountsToDB();

//    Verification
    Assert.assertEquals("table id was not properly set.", t1.getId(), result1.getTableId());
    Assert.assertEquals("table id was not properly set.", t2.getId(), result2.getTableId());

    TSDaily savedTSdaily1 = TSDaily.builder().id(10).date(date)
        .readCount(2)
        .writeCount(2)
        .bytesRead(2l)
        .bytesWritten(2l)
        .recordsRead(2l)
        .recordsWritten(2l)
        .tableId(t1.getId())
        .build();

    TSDaily savedTSdaily2 = TSDaily.builder().id(20).date(date)
        .readCount(3)
        .writeCount(3)
        .bytesRead(3l)
        .bytesWritten(3l)
        .recordsRead(3l)
        .recordsWritten(3l)
        .tableId(t2.getId())
        .build();

    TSDaily savedTSdaily3 = TSDaily.builder().id(null).date(date)
        .readCount(4)
        .writeCount(4)
        .bytesRead(4l)
        .bytesWritten(4l)
        .recordsRead(4l)
        .recordsWritten(4l)
        .tableId(t4.getId())
        .build();

    ArgumentCaptor<TSDaily> argument = ArgumentCaptor.forClass(TSDaily.class);

    verify(dailyTableStats, times(3)).save(argument.capture());
    List<TSDaily> capturedValues = argument.getAllValues();
    Assert.assertTrue(capturedValues.contains(savedTSdaily1));
    Assert.assertTrue(capturedValues.contains(savedTSdaily2));
    Assert.assertTrue(capturedValues.contains(savedTSdaily3));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Integer>> tableIdsCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<LocalDate> localDateCaptor1 = ArgumentCaptor.forClass(LocalDate.class);
    ArgumentCaptor<LocalDate> localDateCaptor2 = ArgumentCaptor.forClass(LocalDate.class);
    verify(dailyTableStats, times(1)).findByTablesAndTimeRange(tableIdsCaptor.capture(), localDateCaptor1.capture(), localDateCaptor2.capture());
    Assert.assertTrue("Some of the table Ids were not searched stats table", tableIdsCaptor.getValue().containsAll(Arrays.asList(1, 2, 4)));
    Assert.assertTrue("Table with id 3 should not be captured", !tableIdsCaptor.getValue().contains(3));
    Assert.assertEquals("start date was not correct.", date, localDateCaptor1.getValue());
    Assert.assertEquals("end date was not correct.", date, localDateCaptor2.getValue());
  }
}
