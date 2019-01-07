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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Singleton
public class TableComparatorForDiff{

  private ColumnComparatorForDiff columnComparatorForDiff;
  private Provider<ColumnRepository> columnRepositoryProvider;

  @Inject
  public TableComparatorForDiff(ColumnComparatorForDiff columnComparatorForDiff, Provider<ColumnRepository> columnRepositoryProvider){
    this.columnComparatorForDiff = columnComparatorForDiff;
    this.columnRepositoryProvider = columnRepositoryProvider;
  }
  /**
   * Compares the tables and returns what changes needs to be done in originalTable to make it equivalent to newTableSpec
   * @param originalTable : the original table in the database
   * @param newTableSpec : how the final table should look like.
   * @return : Optional.empty() if both are same. TableDiff with appropriate values set if any diff is found.
   */
  public Optional<TableDiff> diffAndUpdate(Table originalTable, List<Column> oldColumns, Table newTableSpec, Collection<Column> newColumns) {
    Optional<ColumnsDiff> columnsDiff = findColumnsDiff(oldColumns, newColumns);
    Optional<Table> table = diffAndUpdateTable(originalTable, newTableSpec);
    if(!table.isPresent() && !columnsDiff.isPresent()){
      return Optional.empty();
    }else{
      return Optional.of(new TableDiff(table, columnsDiff));
    }
  }

  private Optional<Table> diffAndUpdateTable(Table originalTable, Table newTableSpec) {
    boolean changed = false;
    if (originalTable.getOwner() != null ? !originalTable.getOwner().equals(newTableSpec.getOwner()) : newTableSpec.getOwner() != null) {
      changed = true;
      originalTable.setOwner(newTableSpec.getOwner());
    }
    if (originalTable.getCreateTime() != null ? !originalTable.getCreateTime().equals(newTableSpec.getCreateTime()) : newTableSpec.getCreateTime() != null) {
      changed = true;
      originalTable.setCreateTime(newTableSpec.getCreateTime());
    }
    if (originalTable.getLastAccessTime() != null ? !originalTable.getLastAccessTime().equals(newTableSpec.getLastAccessTime()) : newTableSpec.getLastAccessTime() != null){
        changed = true;
        originalTable.setLastAccessTime(newTableSpec.getLastAccessTime());
    }
    if (originalTable.getTableType() != null ? !originalTable.getTableType().equals(newTableSpec.getTableType()) : newTableSpec.getTableType() != null) {
      changed = true;
      originalTable.setTableType(newTableSpec.getTableType());
    }
    if (originalTable.getLocation() != null ? !originalTable.getLocation().equals(newTableSpec.getLocation()) : newTableSpec.getLocation() != null) {
      changed = true;
      originalTable.setLocation(newTableSpec.getLocation());
    }
    if (originalTable.getSerde() != null ? !originalTable.getSerde().equals(newTableSpec.getSerde()) : newTableSpec.getSerde() != null) {
      changed = true;
      originalTable.setSerde(newTableSpec.getSerde());
    }
    if (originalTable.getInputFormat() != null ? !originalTable.getInputFormat().equals(newTableSpec.getInputFormat()) : newTableSpec.getInputFormat() != null) {
      changed = true;
      originalTable.setInputFormat(newTableSpec.getInputFormat());
    }
    if (originalTable.getOutputFormat() != null ? !originalTable.getOutputFormat().equals(newTableSpec.getOutputFormat()) : newTableSpec.getOutputFormat() != null) {
      changed = true;
      originalTable.setOutputFormat(newTableSpec.getOutputFormat());
    }
    if (originalTable.getCompressed() != null ? !originalTable.getCompressed().equals(newTableSpec.getCompressed()) : newTableSpec.getCompressed() != null) {
      changed = true;
      originalTable.setCompressed(newTableSpec.getCompressed());
    }
    if (originalTable.getNumBuckets() != null ? !originalTable.getNumBuckets().equals(newTableSpec.getNumBuckets()) : newTableSpec.getNumBuckets() != null) {
      changed = true;
      originalTable.setNumBuckets(newTableSpec.getNumBuckets());
    }
    if( originalTable.getComment() != null ? originalTable.getComment().equals(newTableSpec.getComment()) : newTableSpec.getComment() != null){
      changed = true;
      originalTable.setComment(newTableSpec.getComment());
    }

    if(newTableSpec.getProperties() != null) {
      if(originalTable.getProperties() != null) {
        originalTable.getProperties().setAll(newTableSpec.getProperties());
      }
      else {
        originalTable.setProperties(newTableSpec.getProperties());
      }
    }
    if(newTableSpec.getStorageParameters() != null) {
      if(originalTable.getStorageParameters() != null) {
        originalTable.getStorageParameters().setAll(newTableSpec.getStorageParameters());
      }
      else {
        originalTable.setStorageParameters(newTableSpec.getStorageParameters());
      }
    }

    if(changed){
      return Optional.of(originalTable);
    }else{
      return Optional.empty();
    }
  }

  @VisibleForTesting
  Optional<ColumnsDiff> findColumnsDiff(Collection<Column> originalColumns, Collection<Column> newColumns) {
    Map<String, Column> originalColumnsMap = originalColumns.stream()
        .collect(Collectors.toMap(Column::getName, Function.identity()));
    Map<String, Column> newColumnsMap = newColumns.stream()
        .collect(Collectors.toMap(Column::getName, Function.identity()));

    Map<Boolean, List<Column>> partitionedMap = newColumnsMap.values().stream()
        .collect(Collectors.
            partitioningBy(column -> originalColumnsMap.containsKey(column.getName())));
    List<Column> columnsFound = partitionedMap.get(Boolean.TRUE);
    List<Column> columnsAdded = partitionedMap.get(Boolean.FALSE);

    Map<String, Column> updateColumnsMap = columnsFound.stream()
        .map(column -> columnComparatorForDiff.update(originalColumnsMap.get(column.getName()), column))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toMap(Column::getName, Function.identity()));

    Collection<Column> columnsUpdated = updateColumnsMap.values();
    Set<Column> columnsUnchanged = columnsFound.stream()
        .filter(column -> !updateColumnsMap.containsKey(column.getName()))
        .map(column -> originalColumnsMap.get(column.getName()))
        .collect(Collectors.toSet());
    Set<Column> columnsDropped = originalColumnsMap.values().stream()
        .filter(column -> !newColumnsMap.containsKey(column.getName()))
        .collect(Collectors.toSet());

    if(columnsAdded.isEmpty() && columnsDropped.isEmpty() && columnsUpdated.isEmpty()){
      return Optional.empty();
    }else {
      return Optional.of(new ColumnsDiff(columnsUpdated.isEmpty() ? Optional.empty() : Optional.of(columnsUpdated),
                  columnsAdded.isEmpty() ? Optional.empty() : Optional.of(columnsAdded),
                  columnsDropped.isEmpty() ? Optional.empty(): Optional.of(columnsDropped),
                  columnsUnchanged.isEmpty() ? Optional.empty(): Optional.of(columnsUnchanged)));
    }
  }
}
