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

import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnStats;
import com.hortonworks.hivestudio.hive.internal.parsers.TableMetaParserImpl;
import com.hortonworks.hivestudio.hive.resources.jobs.ResultsPaginationController;
import com.hortonworks.hivestudio.hive.resources.jobs.ResultsResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DDLProxyTest {

  @Mock ResultsPaginationController resultsPaginationController;
  @Mock TableMetaParserImpl tableMetaParser;
  @Mock ConnectionSystem connectionSystem;
  @Mock Configuration configuration;
  @Mock JobService jobService;
  @Mock MetaStoreService metaStoreService;
  @Mock ConnectionFactory connectionFactory;

  @Mock HiveContext hiveContext;

  DDLProxy ddlProxy;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    ddlProxy = new DDLProxy(resultsPaginationController, tableMetaParser, connectionSystem, configuration, jobService, metaStoreService, connectionFactory);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void fetchColumnStats() throws Exception {
    ResultsResponse resultsResponse = new ResultsResponse();

    resultsResponse.setHasResults(true);
    resultsResponse.setRows(Arrays.asList(
      new String[]{null, "val0", null, null, null},
      new String[]{ColumnStats.COLUMN_NAME, "", null, null, null},
      new String[]{ColumnStats.DATA_TYPE, "val2", null, null, null},
      new String[]{ColumnStats.MIN, "val3", null, null, null},
      new String[]{ColumnStats.MAX, "val4", null, null, null},
      new String[]{ColumnStats.NUM_NULLS, "val5", null, null, null},
      new String[]{ColumnStats.DISTINCT_COUNT, "", null, null, null},
      new String[]{ColumnStats.AVG_COL_LEN, "val7", null, null, null},
      new String[]{ColumnStats.MAX_COL_LEN, "val8", null, null, null},
      new String[]{ColumnStats.NUM_TRUES, "val9", null, null, null},
      new String[]{ColumnStats.NUM_FALSES, "val10", null, null, null},
      new String[]{ColumnStats.COMMENT, "val11", null, null, null}
    ));

    when(resultsPaginationController.getResult(any(), any(), any(), any(), any(), any(), any())).
      thenReturn(resultsResponse);

    ColumnStats columnStats = ddlProxy.fetchColumnStats("", 1, hiveContext);

    Assert.assertEquals("Invalid value", "val2", columnStats.getDataType());
    Assert.assertEquals("Invalid value", "val3", columnStats.getMin());
    Assert.assertEquals("Invalid value", "val4", columnStats.getMax());
    Assert.assertEquals("Invalid value", "val5", columnStats.getNumNulls());
    Assert.assertNull("Invalid value", columnStats.getDistinctCount());
    Assert.assertEquals("Invalid value", "val7", columnStats.getAvgColLen());
    Assert.assertEquals("Invalid value", "val8", columnStats.getMaxColLen());
    Assert.assertEquals("Invalid value", "val9", columnStats.getNumTrues());
    Assert.assertEquals("Invalid value", "val10", columnStats.getNumFalse());
    Assert.assertEquals("Invalid value", "val11", columnStats.getComment());
  }

}