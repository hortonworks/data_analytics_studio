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

import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.DatabaseComparator;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Optional;

@Slf4j
@Singleton
public class AlterDatabaseEventHandler extends MetaEventHandler{

  @Inject
  public AlterDatabaseEventHandler(Provider<TableRepository> tableRepository,
                                   MetaInfoUtils metaInfoUtils, Provider<ColumnRepository> columnRepository,
                                   Provider<DatabaseRepository> databaseRepository, DatabaseComparator databaseComparator,
                                   Provider<DBReplicationRepository> dbReplicationRepository, HdfsApi hdfsApi) {
    super(tableRepository,
        metaInfoUtils, columnRepository,
        databaseRepository, databaseComparator, dbReplicationRepository, hdfsApi);
  }

  private Optional<Database> updateDatabase(org.apache.hadoop.hive.metastore.api.Database dbObjAfter) {
    Database hsdb = databaseRepository.get().getByDatabaseNameAndNotDropped(dbObjAfter.getName());
    Optional<Database> hsDatabase = databaseComparator.diffAndUpdateFromHiveDatabase(dbObjAfter, hsdb);
    return hsDatabase.map(database -> databaseRepository.get().save(database));
  }

  public Optional<Database> alterDatabase(String payload, Integer id) throws Exception {
    MessageDeserializer deserializer = MessageFactory.getInstance().getDeserializer();
    AlterDatabaseMessage alterDatabaseMessage = deserializer.getAlterDatabaseMessage(payload);
    org.apache.hadoop.hive.metastore.api.Database dbObjAfter = alterDatabaseMessage.getDbObjAfter();
    log.info("dbObjAfter : {}", dbObjAfter);
    return persistReplicationEvent(() -> updateDatabase(dbObjAfter), id);
  }
}
