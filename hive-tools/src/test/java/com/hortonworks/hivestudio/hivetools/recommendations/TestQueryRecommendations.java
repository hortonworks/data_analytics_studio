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

import java.util.HashMap;
import java.util.HashSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hivetools.QueryBase;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Expression;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.ConfigurationAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.DataCastAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.JoinDataTypeAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.StringFunctionAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;

public class TestQueryRecommendations extends QueryBase {

  private QueryRecommendations queryRecommendations;
  private HiveQuery hiveQuery;
  private QueryDetails queryDetails;
  private HashMap<String, ColumnInfo> columnHash;

  private Query query;

  @Before
  public void setUp() throws Exception {
    queryRecommendations = new QueryRecommendations(null, new ConfigurationAnalyzer(), new JoinDataTypeAnalyzer(), new DataCastAnalyzer(), new StringFunctionAnalyzer());

    columnHash = new HashMap<>();

    hiveQuery = new HiveQuery();
    queryDetails = new QueryDetails();

    query = loadQuery(DEFAULT_RESOURCE);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void basicTests() throws Exception {
    Assert.assertEquals("Invalid number of analyzers", 4, queryRecommendations.analyzers.size());
  }

  @Test
  public void getColumnKey() throws Exception {
    Assert.assertEquals("Invalid column key", "db.table.col1", QueryRecommendations.getColumnKey("db", "table", "col1"));
  }

  @Test
  public void getRecommendations() throws Exception {
    HashSet<Recommendation> recommendations;

    // Configuration analyzer check
    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.set("hive.optimize.ppd", BooleanNode.getFalse());
    queryDetails.setConfiguration(node);
    recommendations = queryRecommendations.getRecommendations(hiveQuery, queryDetails, query, columnHash);
    Assert.assertEquals("Didn't get expected number of recommendations", 1, recommendations.size());

    // Data cast analyzer check
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setType("int");
    columnHash.put("default.catalog_sales.cs_quantity", columnInfo);
    recommendations = queryRecommendations.getRecommendations(hiveQuery, queryDetails, query, columnHash);
    Assert.assertEquals("Didn't get expected number of recommendations", 2, recommendations.size());

    // Join data type analyzer check
    columnInfo = new ColumnInfo();
    columnInfo.setType("int");
    columnHash.put("default.catalog_sales.cs_bill_cdemo_sk", columnInfo);
    columnInfo = new ColumnInfo();
    columnInfo.setType("float");
    columnHash.put("default.customer_demographics.cd_demo_sk", columnInfo);
    recommendations = queryRecommendations.getRecommendations(hiveQuery, queryDetails, query, columnHash);
    Assert.assertEquals("Didn't get expected number of recommendations", 3, recommendations.size());

    // String function analyzer check
    query.getFilters().add(new Expression("_col1 + upper(abc)", null, null, null));
    recommendations = queryRecommendations.getRecommendations(hiveQuery, queryDetails, query, columnHash);
    Assert.assertEquals("Didn't get expected number of recommendations", 4, recommendations.size());
  }
}
