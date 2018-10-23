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
package com.hortonworks.hivestudio.hive.internal.generators;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.hortonworks.hivestudio.hive.client.ColumnDescription;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class InsertFromQueryGenerator implements QueryGenerator{
  private InsertFromQueryInput insertFromQueryInput;

  public InsertFromQueryGenerator(InsertFromQueryInput insertFromQueryInput) {
    this.insertFromQueryInput = insertFromQueryInput;
  }

  @Override
  public Optional<String> getQuery()  {
    StringBuilder insertQuery = new StringBuilder();
    //Dynamic partition strict mode requires at least one static partition column. To turn this off set hive.exec.dynamic.partition.mode=nonstrict
    insertQuery.append("set hive.exec.dynamic.partition.mode=nonstrict;").append("\n");

    insertQuery.append(" FROM ").append("`").append(insertFromQueryInput.getFromDatabase()).append("`.`")
        .append(insertFromQueryInput.getFromTable()).append("` tempTable");

    insertQuery.append(" INSERT INTO TABLE `").append(insertFromQueryInput.getToDatabase()).append('`').append(".")
        .append("`").append(insertFromQueryInput.getToTable()).append("`");
        // PARTITION (partcol1[=val1], partcol2[=val2] ...)
        if(insertFromQueryInput.getPartitionedColumns() != null && insertFromQueryInput.getPartitionedColumns().size() > 0){
          insertQuery.append(" PARTITION ").append("(");
          insertQuery.append(Joiner.on(",").join(FluentIterable.from(insertFromQueryInput.getPartitionedColumns()).transform(new Function<ColumnInfo, String>() {
            @Override
            public String apply(ColumnInfo columnInfo) {
              return "`" + columnInfo.getName() + "`";
            }
          })));
          insertQuery.append(" ) ");
        }

    insertQuery.append(" SELECT ");

    List<ColumnInfo> allColumns = new LinkedList<>(insertFromQueryInput.getNormalColumns());
    // this order matters or first normal columns and in the last partitioned columns matters.
    allColumns.addAll(insertFromQueryInput.getPartitionedColumns());
    boolean first = true;
    for(ColumnInfo column : allColumns){
      String type = column.getType();
      boolean unhex = insertFromQueryInput.getUnhexInsert() && (
          ColumnDescription.DataTypes.STRING.toString().equals(type)
              || ColumnDescription.DataTypes.VARCHAR.toString().equals(type)
              || ColumnDescription.DataTypes.CHAR.toString().equals(type)
      );

      if(!first){
        insertQuery.append(", ");
      }

      if(unhex) {
        insertQuery.append("UNHEX(");
      }

      insertQuery.append("tempTable.");
      insertQuery.append('`').append(column.getName()).append('`');

      if(unhex) {
        insertQuery.append(")");
      }

      first = false;
    }

    insertQuery.append(";");
    String query = insertQuery.toString();
    log.info("Insert From Query : {}", query);
    return Optional.of(query);
  }
}
