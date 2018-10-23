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
package com.hortonworks.hivestudio.eventProcessor.meta.handlers;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;

import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaInfoUtils;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.ColumnsDiff;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.DatabaseComparator;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.TableComparatorForDiff;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.TableDiff;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class AlterTableEventHandler extends MetaEventHandler {

  private final TableComparatorForDiff tableComparatorForDiff;

  @Inject
  public AlterTableEventHandler(TableComparatorForDiff tableComparatorForDiff, Provider<TableRepository> tableRepository,
                                MetaInfoUtils metaInfoUtils, Provider<ColumnRepository> columnRepository,
                                Provider<DatabaseRepository> databaseRepository, DatabaseComparator databaseComparator,
                                Provider<DBReplicationRepository> dbReplicationRepository, HdfsApi hdfsApi){
    super(tableRepository,
        metaInfoUtils, columnRepository,
        databaseRepository, databaseComparator, dbReplicationRepository, hdfsApi);
    this.tableComparatorForDiff = tableComparatorForDiff;
  }

  public Optional<Table> alterTable(String payload, Integer id) throws Exception {
    MessageDeserializer deserializer = MessageFactory.getInstance().getDeserializer();
    AlterTableMessage alterTableMessage = deserializer.getAlterTableMessage(payload);
    log.info("Processing alter table of : {}", alterTableMessage.getTable());
    org.apache.hadoop.hive.metastore.api.Table tableObjAfter1 = alterTableMessage.getTableObjAfter();
    Optional<Table> updatedTable = persistReplicationEvent(() -> updateTable(tableObjAfter1), id);
    log.info("Table updation happened for {} : {}", alterTableMessage.getTable(), updatedTable.isPresent());
    return updatedTable;
  }

  private Optional<Table> updateTable(org.apache.hadoop.hive.metastore.api.Table hiveTable, Database database) {
    TableRepository tableRepo = tableRepository.get();
    ColumnRepository columnRepo = columnRepository.get();

    Table originalTable = tableRepo.getByDBNameTableNameAndNotDropped(hiveTable.getDbName(), hiveTable.getTableName());
    List<Column> originalColumns = columnRepo.getAllForTableNotDropped(originalTable.getId());

    Date now = new Date();
    Pair<Table, Collection<Column>> tableSpec = metaInfoUtils.convertHiveTableToHSTable(hiveTable, database, now);

    Optional<TableDiff> tableDiffOptional = tableComparatorForDiff.diffAndUpdate(originalTable, originalColumns, tableSpec.getFirst(), tableSpec.getSecond());

    log.debug("table diff : {}", tableDiffOptional);
    if (tableDiffOptional.isPresent()) {
      TableDiff tableDiff = tableDiffOptional.get();
      Optional<Table> tableOptional = tableDiff.getTable();
      Optional<ColumnsDiff> columnsDiffOptional = tableDiff.getColumnsDiff();
      originalTable.setLastUpdatedAt(now);
      if (tableOptional.isPresent()) {
        tableRepo.save(originalTable);
      }
      if (columnsDiffOptional.isPresent()) {
        ColumnsDiff columnsDiff = columnsDiffOptional.get();
        if (columnsDiff.getColumnsAdded().isPresent()) {
          columnsDiff.getColumnsAdded().get().forEach(column -> {
            column.setTableId(originalTable.getId());
            column.setLastUpdatedAt(now);
            column.setDropped(Boolean.FALSE);
            columnRepo.upsert(column);
          });
        }
        if (columnsDiff.getColumnsDropped().isPresent()) {
          columnsDiff.getColumnsDropped().get().forEach(column -> {
            column.setTableId(originalTable.getId());
            column.setDropped(Boolean.TRUE);
            column.setDroppedAt(now);
            columnRepo.upsert(column);
          });
        }
        if (columnsDiff.getColumnsUpdated().isPresent()) {
          columnsDiff.getColumnsUpdated().get().forEach(column -> {
            column.setTableId(originalTable.getId());
            column.setLastUpdatedAt(now);
            columnRepo.upsert(column);
          });
        }
      }
      return Optional.of(originalTable);
    }
    return Optional.empty();
  }

  private Optional<Table> updateTable(org.apache.hadoop.hive.metastore.api.Table table) {
    Database database = databaseRepository.get().getByDatabaseNameAndNotDropped(table.getDbName());
    return updateTable(table, database);
  }
}
