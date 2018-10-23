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
package com.hortonworks.hivestudio.hivetools.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Column;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Utils {

  protected JsonNode getJSONObjectFromPath(JsonNode baseObject, String... keys) {
    for (String key : keys) {
      if(baseObject.has(key)) {
        baseObject = baseObject.get(key);
      }
      else {
        return null;
      }
    }
    return baseObject;
  }

  protected List<Column> findUnique(List<Column> columns) {
    HashMap<String, Column> columnHash = new HashMap<>();
    ArrayList<Column> uniqueList = new ArrayList<>();

    for (Column column : columns) {
      String extendedColumnName = column.getExtendedName();
      if(!columnHash.containsKey(extendedColumnName)) {
        columnHash.put(extendedColumnName, column);
        uniqueList.add(column);
      }
    }

    return uniqueList;
  }

  public List<Column> extractColumns(List<Expression> expressions) {
    ArrayList<Column> columns = new ArrayList<>();

    expressions.forEach(expression->{
      columns.addAll(expression.getSourceColumns());
    });

    return findUnique(columns);
  }

}
