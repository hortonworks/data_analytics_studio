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

import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.DatabaseComparator;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Optional;

@Slf4j
@Singleton
public class RenameTableEventHandler extends MetaEventHandler{

  @Inject
  public RenameTableEventHandler(Provider<TableRepository> tableRepository,
                                 MetaInfoUtils metaInfoUtils, Provider<ColumnRepository> columnRepository,
                                 Provider<DatabaseRepository> databaseRepository, DatabaseComparator databaseComparator,
                                 Provider<DBReplicationRepository> dbReplicationRepository, HdfsApi hdfsApi) {
    super(tableRepository,
        metaInfoUtils, columnRepository,
        databaseRepository, databaseComparator, dbReplicationRepository, hdfsApi);
  }

  public Optional<Table> renameTable(String payload, Integer id) throws Exception {
    MessageDeserializer deserializer = MessageFactory.getInstance().getDeserializer();
    AlterTableMessage alterTableMsg = deserializer.getAlterTableMessage(payload);
    log.info("Processing rename table of : {}", alterTableMsg.getTable());
    org.apache.hadoop.hive.metastore.api.Table tableObjBefore = alterTableMsg.getTableObjBefore();
    org.apache.hadoop.hive.metastore.api.Table tableObjAfter = alterTableMsg.getTableObjAfter();
    if (!tableObjAfter.getDbName().equals(tableObjBefore.getDbName())) {
      log.error("The table :{} was renamed across DBs fromDB : {}, toDB : {}. We cannot handle this. Ignoring this.", tableObjAfter.getTableName()
          , tableObjBefore.getDbName(), tableObjAfter.getDbName());
      return Optional.empty();
    }
    Table renameTable = persistReplicationEvent(() -> renameTable(tableObjBefore, tableObjAfter), id);
    log.info("Successfully renamed the table to : {}", renameTable.getName());
    return Optional.of(renameTable);
  }

  private Table renameTable(org.apache.hadoop.hive.metastore.api.Table tableObjBefore, org.apache.hadoop.hive.metastore.api.Table tableObjAfter) {
    Table table = tableRepository.get().getByDBNameTableNameAndNotDropped(tableObjBefore.getDbName(), tableObjBefore.getTableName());
    table.setName(tableObjAfter.getTableName());
    return tableRepository.get().save(table);
  }
}
