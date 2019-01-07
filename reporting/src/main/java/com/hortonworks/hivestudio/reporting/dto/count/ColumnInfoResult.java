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
package com.hortonworks.hivestudio.reporting.dto.count;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hortonworks.hivestudio.common.entities.Column;

import lombok.Value;

@Value
public class ColumnInfoResult {
  private final Integer id;
  private final String name;
  private final String datatype;
  private final String columnType;
  private final String comment;

  private final Boolean isPrimary;
  private final Boolean isPartitioned;
  private final Boolean isSortKey;

  public static ColumnInfoResult fromColumnEntity(Column column) {
     return new ColumnInfoResult(column.getId(), column.getName(), column.getDatatype(), column.getColumnType().toString(), column.getComment(), column.getIsPrimary(),
       column.getIsPartitioned(), column.getIsSortKey());
  }

  public static List<ColumnInfoResult> fromColumnEntities(List<Column> columns) {
    return columns == null? Collections.emptyList(): columns.stream().map(ColumnInfoResult::fromColumnEntity).collect(Collectors.toList());
  }
}
