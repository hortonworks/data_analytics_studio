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
package com.hortonworks.hivestudio.hivetools.recommendations;

import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.PartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.StorageInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hive.internal.dto.TableStats;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.table.ColumnDataTypeAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.table.PartitionInfoAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.table.TableMetaAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

public class TestTableRecommendations {

  TableRecommendations tableRecommendations;
  TableMeta meta;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    Configuration configuration = new Configuration(properties);
    tableRecommendations = new TableRecommendations(new ColumnDataTypeAnalyzer(), new TableMetaAnalyzer(),new PartitionInfoAnalyzer(configuration));
    meta = new TableMeta();

    StorageInfo storage = new StorageInfo();
    storage.setCompressed("true");
    storage.setSerdeLibrary("OrcSerde");
    meta.setStorageInfo(storage);

    TableStats stats = new TableStats();
    stats.setTableStatsEnabled(true);
    meta.setTableStats(stats);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void basicTests() throws Exception {
    Assert.assertEquals("Invalid number of analyzers", 3, tableRecommendations.analyzers.size());
  }

  @Test
  public void getRecommendations() throws Exception {
    HashSet<Recommendation> recommendations;

    ArrayList<TablePartitionInfo> partitionInfos = new ArrayList<>();

    // Column data type analyzer check
    ArrayList<ColumnInfo> columns = new ArrayList<>();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setType("varchar");
    columns.add(columnInfo);
    meta.setColumns(columns);
    meta.setPartitionInfo(new PartitionInfo(new ArrayList<>()));

    recommendations = tableRecommendations.getRecommendations(meta, partitionInfos);
    Assert.assertEquals("Didn't get expected number of recommendations", 1, recommendations.size());

    // Table meta analyzer check
    meta.getStorageInfo().setSerdeLibrary("LazySimpleSerDe");
    recommendations = tableRecommendations.getRecommendations(meta, partitionInfos);
    Assert.assertEquals("Didn't get expected number of recommendations", 2, recommendations.size());
  }
}