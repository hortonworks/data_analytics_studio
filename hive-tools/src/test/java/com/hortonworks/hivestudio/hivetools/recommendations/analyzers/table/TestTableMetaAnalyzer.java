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
package com.hortonworks.hivestudio.hivetools.recommendations.analyzers.table;

import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.DetailedTableInfo;
import com.hortonworks.hivestudio.hive.internal.dto.PartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.StorageInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;

import com.hortonworks.hivestudio.hive.internal.dto.TableStats;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class TestTableMetaAnalyzer {

  private TableMetaAnalyzer tableMetaAnalyzer;
  private TableMeta table;
  private ArrayList<TablePartitionInfo> tablePartitionInfos;

  @Before
  public void setUp() throws Exception {
    tableMetaAnalyzer = new TableMetaAnalyzer();
    tablePartitionInfos = new ArrayList<>();

    table = new TableMeta();
    table.setTable("t1");

    StorageInfo storage = new StorageInfo();
    storage.setCompressed("");
    storage.setSerdeLibrary("");
    table.setStorageInfo(storage);

    TableStats stats = new TableStats();
    stats.setTableStatsEnabled(true);
    table.setTableStats(stats);

    DetailedTableInfo detailedInfo = new DetailedTableInfo();
    detailedInfo.setParameters(new HashMap<>());
    table.setDetailedInfo(detailedInfo);

    ArrayList<ColumnInfo> columns = new ArrayList<>(Arrays.asList(new ColumnInfo(), new ColumnInfo(), new ColumnInfo()));
    columns.get(0).setName("col1");
    columns.get(1).setName("col2");
    columns.get(2).setName("col3");
    PartitionInfo partitionInfo = new PartitionInfo(columns);
    table.setPartitionInfo(partitionInfo);
  }

  @Test
  public void analyzeORC() throws Exception {
    Assert.assertEquals("Recommendations not generated", 1, tableMetaAnalyzer.analyze(table, tablePartitionInfos).size());
    table.getStorageInfo().setSerdeLibrary("OrcSerde");
    Assert.assertEquals("Recommendations shouldn't be generated", 0, tableMetaAnalyzer.analyze(table, tablePartitionInfos).size());
  }

  @Test
  public void analyzeStats() throws Exception {
    Assert.assertEquals("Stats recommendation shouldn't have been generated", 1, tableMetaAnalyzer.analyze(table, tablePartitionInfos).size());

    table.getTableStats().setTableStatsEnabled(false); // With partition
    String expectedRecMsg = String.format(TableMetaAnalyzer.STATS_WITH_PARTITION_MSG, table.getTable(), "col1, col2, col3");
    Assert.assertEquals("Stats recommendation not generated", expectedRecMsg, tableMetaAnalyzer.analyze(table, tablePartitionInfos).get(1).getMessage());

    table.getPartitionInfo().getColumns().clear(); // Without partition
    expectedRecMsg = String.format(TableMetaAnalyzer.STATS_WITHOUT_PARTITION_MSG, table.getTable());
    Assert.assertEquals("Stats recommendation not generated", expectedRecMsg, tableMetaAnalyzer.analyze(table, tablePartitionInfos).get(1).getMessage());
  }

}