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

import com.google.common.collect.ImmutableSet;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Expression;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class StringFunctionAnalyzer implements QueryAnalyzer {
  private static final Set<String> OBNOXIOUS_FUNCTIONS = ImmutableSet.of(
    "ucase", "lcase", "upper", "lower");

  private static final String STR_FUN_MSG = "The query used %1$s(). Those functions are usually slow when used during query run time. You should consider create new columns with the desired type case instead of applying %1$s() function on the fly.";

  private ArrayList<Recommendation> traverseForRecommendations(Expression expression, ASTNode node, ArrayList<Recommendation> recommendations, HashMap<String, ColumnInfo> columnHash) {

    if(node.getType() == HiveParser.TOK_FUNCTION) {
      String functionName = node.getChild(0).getText();
      if(OBNOXIOUS_FUNCTIONS.contains(functionName)) {
        recommendations.add(new Recommendation(String.format(STR_FUN_MSG, functionName)));
      }
    }

    if(node.getChildCount() > 0) {
      node.getChildren().forEach(child -> {
        traverseForRecommendations(expression, (ASTNode) child, recommendations, columnHash);
      });
    }

    return recommendations;
  }

  private ArrayList<Recommendation> analyzeExpressions(List<Expression> expressions, ArrayList<Recommendation> recommendations, HashMap<String, ColumnInfo> columnHash) {
    Pattern functionPattern = Pattern.compile(String.join("|", OBNOXIOUS_FUNCTIONS));

    expressions.forEach(expression->{
      if(functionPattern.matcher(expression.getExpressionString()).find()) {
        ASTNode node = expression.getExpressionTree();
        traverseForRecommendations(expression, node, recommendations, columnHash);
      }
    });

    return recommendations;
  }

  public ArrayList<Recommendation> analyze(HiveQuery hiveQuery, QueryDetails queryDetails, Query queryDefinition, HashMap<String, ColumnInfo> columnHash) {
    ArrayList<Recommendation> recommendations = new ArrayList<>();

    analyzeExpressions(queryDefinition.getProjections(), recommendations, columnHash);
    analyzeExpressions(queryDefinition.getFilters(), recommendations, columnHash);
    analyzeExpressions(queryDefinition.getAggregations(), recommendations, columnHash);

    queryDefinition.getJoins().forEach(join->{
      analyzeExpressions(join.getLeftExpressions(), recommendations, columnHash);
      analyzeExpressions(join.getRightExpressions(), recommendations, columnHash);
    });

    return recommendations;
  }
}
