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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.PartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import com.hortonworks.hivestudio.common.entities.Table;

public class PartitionInfoAnalyzerTest {

  PartitionInfoAnalyzer partitionInfoAnalyzer;

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    partitionInfoAnalyzer = new PartitionInfoAnalyzer(new Configuration(properties));
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void checkDefaultValues() throws Exception {
    Assert.assertEquals("Invalid partition count threshold", 1000, partitionInfoAnalyzer.partitionCountThreshold.intValue());
    Assert.assertEquals("Invalid median partition size threshold", 1024, partitionInfoAnalyzer.medianPartitionSizeThresholdInMb.intValue());
    Assert.assertEquals("Invalid file count threshold", 100, partitionInfoAnalyzer.fileCountThreshold.intValue());
    Assert.assertEquals("Invalid file size threshold", 15, partitionInfoAnalyzer.fileSizeThresholdInMb.intValue());
  }

  @Test
  public void analyze() throws Exception {
    Integer tableId = 11;
    ArrayList<TablePartitionInfo> partitionInfos = new ArrayList<>();
    ObjectNode details = new ObjectMapper().createObjectNode();

    TableMeta meta = new TableMeta();
    meta.setTable("t1");

    ColumnInfo colInfo1 = new ColumnInfo("col1", null);
    ColumnInfo colInfo2 = new ColumnInfo("col2", null);
    meta.setPartitionInfo(new PartitionInfo(new ArrayList<>(Arrays.asList(colInfo1, colInfo2))));

    Assert.assertEquals("Shouldn't show any recommendations", 0, partitionInfoAnalyzer.analyze(meta, partitionInfos).size());

    // Many (>1000) small partitions - Generate recommendation
    partitionInfos.clear();
    for(int i=0; i<1100; i++ ) {
      partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v" + i, details, 10l, 100, 10));
    }
    String expectedRecMsg = String.format(PartitionInfoAnalyzer.TOO_MANY_SMALL_PARTITIONS_MSG, meta.getTable(), partitionInfoAnalyzer.partitionCountThreshold, colInfo1.getName() + ", " + colInfo2.getName(), 1);
    Assert.assertEquals("Recommendation not generated", expectedRecMsg, partitionInfoAnalyzer.analyze(meta, partitionInfos).get(0).getMessage());

    // Few small partitions - Don't generate recommendation
    partitionInfos.clear();
    for(int i=0; i<100; i++ ) {
      partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v" + i, details, 10l, 100, 10));
    }
    Assert.assertEquals("Shouldn't show any recommendations", 0, partitionInfoAnalyzer.analyze(meta, partitionInfos).size());

    // Many (>1000) large partitions. Median partition size > 1GB - Don't generate recommendation
    partitionInfos.clear();
    for(int i=0; i<1100; i++ ) {
      partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v" + i, details, 1025l * 1024 * 1024, 100, 10));
    }
    Assert.assertEquals("Shouldn't show any recommendations", 0, partitionInfoAnalyzer.analyze(meta, partitionInfos).size());

    // Median count of files in partitions > 100 & average fileSize < 15MB - Generate recommendation
    partitionInfos.clear();
    partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v1", details, 10l, 100, 100));
    partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v1", details, 10l, 100, 200));
    expectedRecMsg = String.format(PartitionInfoAnalyzer.TOO_MANY_FILES_PER_PARTITIONS_MSG, meta.getTable());
    Assert.assertEquals("Recommendation not generated", expectedRecMsg, partitionInfoAnalyzer.analyze(meta, partitionInfos).get(0).getMessage());

    // Median count of files in partitions < 100 - Don't generate recommendation
    partitionInfos.clear();
    partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v1", details, 10l, 100, 10));
    partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v1", details, 10l, 100, 20));
    Assert.assertEquals("Shouldn't show any recommendations", 0, partitionInfoAnalyzer.analyze(meta, partitionInfos).size());

    // Median count of files in partitions > 100 & average fileSize > 15MB - Dont generate recommendation
    partitionInfos.clear();
    partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v1", details, 16l * 1024 * 1024 * 100, 100, 100));
    partitionInfos.add(new TablePartitionInfo(1, tableId, "c1=v1", details, 16l * 1024 * 1024 * 200, 100, 200));
    Assert.assertEquals("Shouldn't show any recommendations", 0, partitionInfoAnalyzer.analyze(meta, partitionInfos).size());
  }

}