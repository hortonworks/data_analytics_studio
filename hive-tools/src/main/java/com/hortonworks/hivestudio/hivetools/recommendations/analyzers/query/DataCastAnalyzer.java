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
import com.hortonworks.hivestudio.hivetools.parsers.entities.Column;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Expression;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.recommendations.QueryRecommendations;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class DataCastAnalyzer implements QueryAnalyzer {
  private static final Set<String> OBNOXIOUS_FUNCTIONS = ImmutableSet.of(
      "UDFToDouble", "UDFToFloat", "UDFToString", "CAST");

  private static final String DATA_CAST_MSG = "The query attempts to cast %s.%s from %s to %s. Casting is usually computationally expensive. You should considering creating a new column in %s.";

  private ArrayList<Recommendation> traverseForRecommendations(Expression expression, ASTNode node, ArrayList<Recommendation> recommendations, HashMap<String, ColumnInfo> columnHash) {
    String columnName = null;
    String toDataType = null;
    if(node.getType() == HiveParser.TOK_FUNCTION) {
      Tree firstChild = node.getChild(0);
      Tree secondChild = node.getChild(1);

      if(secondChild.getType() == HiveParser.TOK_TABLE_OR_COL) {
        columnName = Expression.deNormalizeExpression(secondChild.getChild(0).getText());
      }

      switch(firstChild.getType()) {
        case HiveParser.Identifier:
          String functionName = firstChild.getText();
          switch(functionName) {
            case "UDFToDouble":
              toDataType = "double";
              break;
            case "UDFToFloat":
              toDataType = "float";
              break;
            case "UDFToString":
              toDataType = "string";
              break;
            case "UDFToDate":
              toDataType = "date";
              break;
          }
          break;
        case HiveParser.TOK_INT:
          toDataType = "int";
          break;
        case HiveParser.TOK_DECIMAL:
          toDataType = "decimal";
          break;
        case HiveParser.TOK_DOUBLE:
          toDataType = "double";
          break;
        case HiveParser.TOK_FLOAT:
          toDataType = "float";
          break;
        case HiveParser.TOK_STRING:
          toDataType = "string";
          break;
        case HiveParser.TOK_DATE:
          toDataType = "date";
          break;
      }

      ArrayList<Column> sourceColumns = expression.getSourceColumns(columnName);

      // We don't throw error if there are multiple (Derived) columns or no (Invalid case) columns
      if(toDataType != null && sourceColumns.size() == 1) {
        Column column = sourceColumns.get(0);
        String columnKey = QueryRecommendations.getColumnKey(column.getTable().getDatabaseName(), column.getTable().getName(), column.getColumnName());
        ColumnInfo info = columnHash.get(columnKey);

        if(info != null) {
          String message = String.format(DATA_CAST_MSG, column.getTable().getName(), column.getColumnName(), info.getType(), toDataType, toDataType);
          recommendations.add(new Recommendation(message));
        }
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
