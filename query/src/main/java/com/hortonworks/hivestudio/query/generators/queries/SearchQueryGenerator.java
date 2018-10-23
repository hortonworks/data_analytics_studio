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
package com.hortonworks.hivestudio.query.generators.queries;

import java.util.List;

import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.orm.EntityField;
import com.hortonworks.hivestudio.common.orm.EntityTable;

/**
 * Generates the Native query for hive and dag info search
 */
public class SearchQueryGenerator {

  private final String offsetParameterBinding;
  private final String limitParameterBinding;
  private final String highlightQueryFunction;
  private final List<String> predicates;
  private final String sortFragment;
  private final EntityTable hiveQuery;
  private final EntityTable dagInfo;

  public SearchQueryGenerator(EntityTable hiveQuery, EntityTable dagInfo,
      String offsetParameterBinding, String limitParameterBinding,
      String highlightQueryFunction, String sortFragment, List<String> predicates) {
    this.hiveQuery = hiveQuery;
    this.dagInfo = dagInfo;
    this.offsetParameterBinding = offsetParameterBinding;
    this.limitParameterBinding = limitParameterBinding;
    this.highlightQueryFunction = highlightQueryFunction;
    this.predicates = predicates;
    this.sortFragment = sortFragment;
  }

  private static void addFields(StringBuilder builder, EntityTable table) {
    for (EntityField field : table.getFields()) {
      builder.append(field.getEntityPrefix());
      builder.append(".");
      builder.append(field.getDbFieldName());
      builder.append(" AS ");
      builder.append(field.getEntityPrefix());
      builder.append(field.getDbFieldName());
      builder.append(", ");
    }
  }

  private static void addTable(StringBuilder builder, String schema, EntityTable table) {
    builder.append(schema);
    builder.append('.');
    builder.append(table.getTableName());
    builder.append(' ');
    builder.append(table.getTablePrefix());
  }

  public String generate() {
    String schema = Constants.DATABASE_SCHEMA;
    StringBuilder builder = new StringBuilder(2048);
    builder.append("SELECT ");
    addFields(builder, hiveQuery);
    addFields(builder, dagInfo);
    builder.append(highlightQueryFunction);
    builder.append(" FROM ");
    addTable(builder, schema, hiveQuery);
    builder.append(" LEFT OUTER JOIN ");
    addTable(builder, schema, dagInfo);
    builder.append(" ON ");
    builder.append(hiveQuery.getTablePrefix());
    builder.append(".id = ");
    builder.append(dagInfo.getTablePrefix());
    builder.append(".hive_query_id");
    boolean isFirst = true;
    for (String predicate : predicates) {
      if (isFirst) {
        builder.append(" WHERE ");
        isFirst = false;
      } else {
        builder.append(" AND ");
      }
      builder.append(predicate);
    }
    builder.append(' ');
    builder.append(sortFragment);
    builder.append(" OFFSET :");
    builder.append(offsetParameterBinding);
    builder.append(" LIMIT :");
    builder.append(limitParameterBinding);
    return builder.toString();
  }
}
