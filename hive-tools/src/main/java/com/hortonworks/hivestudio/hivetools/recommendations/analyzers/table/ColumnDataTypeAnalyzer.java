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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;

import java.util.ArrayList;
import java.util.Collection;

public class ColumnDataTypeAnalyzer implements TableAnalyzer {

  @VisibleForTesting
  static final String STR_TYPE_MSG = "Please use STRING type instead of %s for column %s.%s. STRING type usually performs better.",
      DECIMAL_TO_INT_MSG = "Converting %s.%s to INT/BIGINT will speed up your query because it’s currently using Decimal type. Decimal type is relatively expensive.",
      COMPLEX_TYPE_MSG = "Column %s.%s is using complex data type. While convenient, complex datatypes such as nested data structure is expensive to store and process. Please consider flatten the data structure into columns in a table or multiple tables.";

  @VisibleForTesting
  String normalizeDataType(String dataType) {
    if(dataType != null) {
      // Because complex types comes suffixed with some meta. Eg: array<double>
      if(dataType.contains("<")) {
        dataType = dataType.substring(0, dataType.indexOf("<"));
      }

      dataType = dataType.toLowerCase();
    }
    return dataType;
  }

  @VisibleForTesting
  ArrayList<Recommendation> analyzeColumn(TableMeta table, ColumnInfo column) {
    ArrayList<Recommendation> recommendations = new ArrayList<>();
    String dataType = normalizeDataType(column.getType());

    switch(dataType) {
      case "varchar":
      case "char":
        recommendations.add(new Recommendation(String.format(STR_TYPE_MSG, dataType.toUpperCase(), table.getTable(), column.getName())));
        break;

      case "decimal":
        if(column.getPrecision() <= 18 && column.getScale() == 0) {
          recommendations.add(new Recommendation(String.format(DECIMAL_TO_INT_MSG, table.getTable(), column.getName())));
        }
        break;

      case "array":
      case "map":
      case "struct":
      case "uniontype":
        recommendations.add(new Recommendation(String.format(COMPLEX_TYPE_MSG, table.getTable(), column.getName())));
        break;
    }

    return recommendations;
  }

  public ArrayList<Recommendation> analyze(TableMeta table, Collection<TablePartitionInfo> partitions) {
    ArrayList<Recommendation> recommendations = new ArrayList<>();

    for (ColumnInfo columnInfo : table.getColumns()) {
      recommendations.addAll(analyzeColumn(table, columnInfo));
    }

    return recommendations;
  }

}
