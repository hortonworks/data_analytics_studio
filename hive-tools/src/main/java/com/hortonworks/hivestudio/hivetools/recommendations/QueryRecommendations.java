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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hivetools.parsers.QueryPlanParser;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.ConfigurationAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.DataCastAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.JoinDataTypeAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.QueryAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query.StringFunctionAnalyzer;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

@Singleton
public class QueryRecommendations {
  private QueryPlanParser queryPlanParser;

  @VisibleForTesting
  final ArrayList<QueryAnalyzer> analyzers;

  public static String getColumnKey(String dbName, String tableName, String columnName) {
    return String.format("%s.%s.%s", dbName, tableName, columnName);
  }

  @Inject
  public QueryRecommendations(QueryPlanParser queryPlanParser,
                              ConfigurationAnalyzer configurationAnalyzer,
                              JoinDataTypeAnalyzer joinDataTypeAnalyzer,
                              DataCastAnalyzer dataCastAnalyzer,
                              StringFunctionAnalyzer stringFunctionAnalyzer) {
    this.queryPlanParser = queryPlanParser;
    this.analyzers = new ArrayList<>(Arrays.asList(
      configurationAnalyzer, joinDataTypeAnalyzer, dataCastAnalyzer, stringFunctionAnalyzer
    ));
  }

  public HashSet<Recommendation> getRecommendations(HiveQuery hiveQuery, QueryDetails queryDetails, Query queryDefinition, HashMap<String, ColumnInfo> columnHash) {
    HashSet<Recommendation> recommendations = new HashSet<>();

    for (QueryAnalyzer analyzer : analyzers) {
      recommendations.addAll(analyzer.analyze(hiveQuery, queryDetails, queryDefinition, columnHash));
    }

    for (Recommendation recommendation : recommendations) {
      recommendation.setType("query");
    }

    return recommendations;
  }

  public HashSet<Recommendation> getRecommendations(HiveQuery hiveQuery, QueryDetails queryDetails, HashMap<String, ColumnInfo> columnHash) throws Exception {
    ObjectNode explainPlan = queryDetails.getExplainPlan();

    Query queryDefinition = queryPlanParser.parse(explainPlan, null);
    return getRecommendations(hiveQuery, queryDetails, queryDefinition, columnHash);
  }
}
