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
package com.hortonworks.hivestudio.eventProcessor.meta.diff;

import com.hortonworks.hivestudio.common.entities.Column;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class ColumnComparatorForDiff {
  /**
   * updates and returns existingColumn with changes as made in changedColumn. does not compare the table, dropped, droppedAt,
   * lastUpdatedAt fields.
   * throws exception if name of columns differs.
   * @param existingColumn
   * @param changedColumn
   * @return : empty if both are same. updated existingColumn in case it differs from changedColumn
   */
  public Optional<Column> update(Column existingColumn, Column changedColumn){
    boolean changed = false;

    if (existingColumn.getName() != null ? !existingColumn.getName().equals(changedColumn.getName()) : changedColumn.getName() != null) {
      throw new IllegalArgumentException("not the same column as the name differs.");
    }

    if (existingColumn.getDatatype() != null ? !existingColumn.getDatatype().equals(changedColumn.getDatatype()) : changedColumn.getDatatype() != null) {
     existingColumn.setDatatype(changedColumn.getDatatype());
     changed = true;
    }

    if (existingColumn.getColumnType() != null ? !existingColumn.getColumnType().equals(changedColumn.getColumnType()) : changedColumn.getColumnType() != null) {
      existingColumn.setColumnType(changedColumn.getColumnType());
      changed = true;
    }

    if (existingColumn.getPrecision() != null ? !existingColumn.getPrecision().equals(changedColumn.getPrecision()) : changedColumn.getPrecision() != null) {
     existingColumn.setPrecision(changedColumn.getPrecision());
     changed = true;
    }

    if (existingColumn.getScale() != null ? !existingColumn.getScale().equals(changedColumn.getScale()) : changedColumn.getScale() != null) {
     existingColumn.setScale(changedColumn.getScale());
     changed = true;
    }

    if (existingColumn.getComment()!= null ? !existingColumn.getComment().equals(changedColumn.getComment()) : changedColumn.getComment() != null) {
      existingColumn.setComment(changedColumn.getComment());
      changed = true;
    }
    if (existingColumn.getIsPrimary() != null ? !existingColumn.getIsPrimary().equals(changedColumn.getIsPrimary()) : changedColumn.getIsPrimary() != null) {
      existingColumn.setIsPrimary(changedColumn.getIsPrimary());
      changed = true;
    }
    if (existingColumn.getIsPartitioned() != null ? !existingColumn.getIsPartitioned().equals(changedColumn.getIsPartitioned()) :
        changedColumn.getIsPartitioned() != null){
      existingColumn.setIsPartitioned(changedColumn.getIsPartitioned());
      changed = true;
    }
    if (existingColumn.getIsClustered() != null ? !existingColumn.getIsClustered().equals(changedColumn.getIsClustered()) :
        changedColumn.getIsClustered() != null){
      existingColumn.setIsClustered(changedColumn.getIsClustered());
      changed = true;
    }
    if (existingColumn.getSortOrder() != null ? !existingColumn.getSortOrder().equals(changedColumn.getSortOrder()) :
        changedColumn.getSortOrder() != null){
      existingColumn.setSortOrder(changedColumn.getSortOrder());
      changed = true;
    }
//    if (isSortKey != null ? !isSortKey.equals(column.isSortKey) : column.isSortKey != null) return false;
//    if (dropped != null ? !dropped.equals(column.dropped) : column.dropped != null) return false;
//    if (droppedAt != null ? !droppedAt.equals(column.droppedAt) : column.droppedAt != null) return false;
//    if (lastUpdatedAt != null ? !lastUpdatedAt.equals(column.lastUpdatedAt) : column.lastUpdatedAt != null)
//      return false;
//    table != null ? table.equals(column.table) : column.table == null;

    if(changed) return Optional.of(existingColumn);
    else return Optional.empty();
  }
}
