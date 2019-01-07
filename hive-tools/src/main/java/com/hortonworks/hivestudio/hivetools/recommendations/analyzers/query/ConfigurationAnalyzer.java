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
package com.hortonworks.hivestudio.hivetools.recommendations.analyzers.query;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import com.hortonworks.hivestudio.common.entities.HiveQuery;

import java.util.ArrayList;
import java.util.HashMap;

public class ConfigurationAnalyzer implements QueryAnalyzer {

  private static final String CONF_PPD_KEY = "hive.optimize.ppd";
  private static final String CONF_VECTORIZATION_KEY = "hive.vectorized.execution.enabled";
  private static final String CONF_CBO_KEY = "hive.cbo.enable";

  private static final String PPD_MSG = "Predicate push down (PPD) is disabled. Hive uses predicates to reduce the amount of data scanned during query. Add SET hive.optimize.ppd=true; and SET hive.optimize.ppd.storage=true; to the beginning of your query or change global settings of HIVE by applying the above changes to speed up your query.";
  private static final String VECTORIZATION_MSG = "Vectorized mode of query execution is disabled. Vectorized query execution processes data in batches of 1024 rows instead of one by one. Add SET hive.vectorized.execution.enabled=true; and SET hive.vectorized.execution.reduce.enabled=true; to the beginning of your query or change global settings of HIVE by applying the above changes to speed up your query.";
  private static final String CBO_MSG = "Cost based optimization (CBO) is disabled. Hive uses CBO to determines which execution plan is most efficient by considering available access paths and by factoring in information based on statistics for the schema objects. Add SET hive.cbo.enable=true; to the beginning of your query or change global settings of HIVE by applying the above changes to speed up your query.";

  public ArrayList<Recommendation> analyze(HiveQuery hiveQuery, QueryDetails queryDetails, Query queryDefinition, HashMap<String, ColumnInfo> columnHash) {
    ArrayList<Recommendation> recommendations = new ArrayList<>();

    ObjectNode configuration = queryDetails.getConfiguration();

    if(configuration != null) {
      if(configuration.has(CONF_PPD_KEY) && configuration.get(CONF_PPD_KEY).asBoolean() != true) {
        recommendations.add(new Recommendation(PPD_MSG));
      }

      if(configuration.has(CONF_VECTORIZATION_KEY) && configuration.get(CONF_VECTORIZATION_KEY).asBoolean() != true) {
        recommendations.add(new Recommendation(VECTORIZATION_MSG));
      }

      if(configuration.has(CONF_CBO_KEY) && configuration.get(CONF_CBO_KEY).asBoolean() != true) {
        recommendations.add(new Recommendation(CBO_MSG));
      }
    }

    return recommendations;
  }
}
