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

import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Column;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Expression;
import com.hortonworks.hivestudio.hivetools.parsers.entities.JoinLink;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.recommendations.QueryRecommendations;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import com.hortonworks.hivestudio.common.entities.HiveQuery;

import java.util.ArrayList;
import java.util.HashMap;

public class JoinDataTypeAnalyzer implements QueryAnalyzer {

  private static final String JOIN_DATA_TYPE_MSG = "The query attempts to join %s.%s of %s to %s.%s of %s. Joining between different data types is computationally expensive as data conversion needs to happen on the fly. You should create new columns within tables and pre-convert the join columns into the same data types.";

  public ArrayList<Recommendation> analyze(HiveQuery hiveQuery, QueryDetails queryDetails, Query queryDefinition, HashMap<String, ColumnInfo> columnHash) {
    ArrayList<Recommendation> recommendations = new ArrayList<>();

    queryDefinition.getJoins().forEach(join->{
      ArrayList<Expression> leftExpressions = join.getLeftExpressions();
      ArrayList<Expression> rightExpressions = join.getRightExpressions();

      if(leftExpressions.size() == 1) {
        ArrayList<Column> leftColumns = leftExpressions.get(0).getSourceColumns();
        ArrayList<Column> rightColumns = rightExpressions.get(0).getSourceColumns();

        if(leftColumns.size() == 1 && rightColumns.size() == 1) {
          Column leftColumn = leftColumns.get(0);
          ColumnInfo leftColInfo = columnHash.get(QueryRecommendations.getColumnKey(leftColumn.getTable().getDatabaseName(), leftColumn.getTable().getName(), leftColumn.getColumnName()));

          Column rightColumn = rightColumns.get(0);
          ColumnInfo rightColInfo = columnHash.get(QueryRecommendations.getColumnKey(rightColumn.getTable().getDatabaseName(), rightColumn.getTable().getName(), rightColumn.getColumnName()));

          if(leftColInfo != null && rightColInfo != null && !leftColInfo.getType().equals(rightColInfo.getType())) {
            String message = String.format(JOIN_DATA_TYPE_MSG,
              leftColumn.getTable().getName(), leftColumn.getColumnName(), leftColInfo.getType(),
              rightColumn.getTable().getName(), rightColumn.getColumnName(), rightColInfo.getType()
            );
            recommendations.add(new Recommendation(message));
          }
        }
      }
    });

    return recommendations;
  }

}
