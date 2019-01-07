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
package com.hortonworks.hivestudio.eventProcessor.meta;

import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.ALL_DB_STAR;
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.DUMPMETADATA;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpInfo;
import com.hortonworks.hivestudio.common.entities.DBReplicationEntity;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.eventProcessor.configuration.Constants;
import com.hortonworks.hivestudio.eventProcessor.configuration.EventProcessingConfig;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.AlterDatabaseEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.AlterTableEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.BootstrapEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.CreateDatabaseEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.CreateTableEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.DropDatabaseEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.DropTableEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.PartitionEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.RenameTableEventHandler;

import lombok.extern.slf4j.Slf4j;

@Singleton
@Slf4j
public class MetaInfoUpdater {
  private final DropDatabaseEventHandler dropDatabaseEventHandler;
  private final AlterDatabaseEventHandler alterDatabaseEventHandler;
  private final HiveRepl hiveRepl;
  private final CreateTableEventHandler createTableEventHandler;
  private HdfsApi hdfsApi;
  private final CreateDatabaseEventHandler createDatabaseEventHandler;
  private final RenameTableEventHandler renameTableEventHandler;
  private final DropTableEventHandler dropTableEventHandler;
  private HiveConf hiveConf;
  private AlterTableEventHandler alterTableEventHandler;
  private BootstrapEventHandler bootstrapEventHandler;
  private PartitionEventHandler partitionEventHandler;

  private Provider<DBReplicationRepository> dbReplicationRepositoryProvider;

  private EventProcessingConfig eventProcessingConfig;

  @Inject
  public MetaInfoUpdater(DropDatabaseEventHandler dropDatabaseEventHandler,
      AlterDatabaseEventHandler alterDatabaseEventHandler,
      HiveRepl hiveRepl, CreateTableEventHandler createTableEventHandler,
      HdfsApi hdfsApi, AlterTableEventHandler alterTableEventHandler,
      CreateDatabaseEventHandler createDatabaseEventHandler,
      RenameTableEventHandler renameTableEventHandler, DropTableEventHandler dropTableEventHandler,
      BootstrapEventHandler bootstrapEventHandler, PartitionEventHandler partitionEventHandler,
      Configuration hadoopConfiguration, HiveConfiguration hiveConfiguration,
      MetaInfoUtils metaInfoUtils, Provider<DBReplicationRepository> dbReplicationRepository,
      EventProcessingConfig eventProcessingConfig) {
    this.dropDatabaseEventHandler = dropDatabaseEventHandler;
    this.alterDatabaseEventHandler = alterDatabaseEventHandler;
    this.createTableEventHandler = createTableEventHandler;
    this.hdfsApi = hdfsApi;
    this.createDatabaseEventHandler = createDatabaseEventHandler;
    this.renameTableEventHandler = renameTableEventHandler;
    this.dropTableEventHandler = dropTableEventHandler;
    this.bootstrapEventHandler = bootstrapEventHandler;
    this.alterTableEventHandler = alterTableEventHandler;
    this.partitionEventHandler = partitionEventHandler;
    this.hiveRepl = hiveRepl;

    this.dbReplicationRepositoryProvider = dbReplicationRepository;
    this.eventProcessingConfig = eventProcessingConfig;

    this.hiveConf = metaInfoUtils.createHiveConfs(hadoopConfiguration);
    hiveConfiguration.keySet().forEach(key -> hiveConf.set(key.toString(), hiveConfiguration.get(key.toString()).get()));
  }




  public List<DBAndTables> bootstrapWarehouseFromDump() {
    WarehouseDumpInfo bootStrapDump = null;
    try {
      bootStrapDump = hiveRepl.getWarehouseBootstrapDump();
      log.info("Fecthed bootstrap dump : {}", bootStrapDump);
      FileStatus[] listdir = hdfsApi.listdir(bootStrapDump.getDumpPath());
      log.info("found following in the dump path {} : {}", bootStrapDump.getDumpPath(), listdir);
      List<DBAndTables> dbAndTables =  bootstrapEventHandler.createWarehouseFromBootstrap(listdir, Integer.valueOf(bootStrapDump.getLastReplicationId()));
      log.info("Successfully created DB : {} and updated DBReplicationEntity with id : {}", ALL_DB_STAR, bootStrapDump.getLastReplicationId());
      return dbAndTables;
    } catch (Exception e) {
      log.error("Exception occurred while creating a new database {}", ALL_DB_STAR, e);
      throw new MetaDataUpdationException(e);
    } finally {
      deleteDump(bootStrapDump);
    }
  }

  private void deleteDump(WarehouseDumpInfo dumpInfo) {
    if (null != dumpInfo)
      this.deleteDump(dumpInfo.getDumpPath());
  }

  private void deleteDump(String dumpPath) {
    if (null != dumpPath) {
      log.info("deleting dump path : {}", dumpPath);
      try {
        hdfsApi.delete(dumpPath, true);
      } catch (IOException | InterruptedException e) {
        log.error("Error occurred while delete the dump path : {}", dumpPath, e);
      }
    }
  }


  public DatabasesAndTables updateWarehouseFromDump(DBReplicationEntity dbReplicationEntity) {
    WarehouseDumpInfo incrementalDump = null;
    dbReplicationRepositoryProvider.get().startReplProcessing(dbReplicationEntity.getDatabaseName());

    try {
      incrementalDump = hiveRepl.getWarehouseIncrementalDump(dbReplicationEntity.getLastReplicationId());
      log.info("received incremental dump : {} ", incrementalDump);
      FileStatus[] listdir = hdfsApi.listdir(incrementalDump.getDumpPath());
      log.debug("Received events paths : {}", (Object[])listdir);
      List<Integer> ids = Arrays.stream(listdir)
          .filter(fileStatus -> !fileStatus.getPath().getName().equals(DUMPMETADATA))
          .map(fileStatus -> fileStatus.getPath().getName())
          .map(Integer::new)
          .sorted()
          .collect(Collectors.toList());

      Set<Table> tables = new HashSet<>();
      Set<Database> databases = new HashSet<>();
      for (Integer id : ids) { // sorted ids
        Path path = new Path(incrementalDump.getDumpPath(), String.valueOf(id));
        log.info("processing dump folder : {}", path);
        DumpMetaData eventDmd = new DumpMetaData(path, this.hiveConf);
        DumpType dumpType = eventDmd.getDumpType();
        String payload = eventDmd.getPayload();

        switch (dumpType) {
          case EVENT_CREATE_DATABASE:
            log.info("Received create database event on path : {}", path);
            Database createdDatabase = createDatabaseEventHandler.createDatabase(new Path(incrementalDump.getDumpPath()), id);
            databases.add(createdDatabase);
            break;
          case EVENT_DROP_DATABASE:
            log.info("Received drop database event on path : {}", path);
            Database database = dropDatabaseEventHandler.dropDatabase(payload, id);
            databases.add(database);
            log.info("dropped database entry {}", database);
            break;
          case EVENT_ALTER_DATABASE:
            log.info("Received EVENT_ALTER_DATABASE on path : {}", path);
            Optional<Database> alteredDatabase = alterDatabaseEventHandler.alterDatabase(payload, id);
            alteredDatabase.map(databases::add);
            break;
          case EVENT_RENAME_TABLE:
            log.info("Received renamed table event on path : {}", path);
            Optional<Table> renameTable = renameTableEventHandler.renameTable(payload, id);
            renameTable.map(tables::add);
            break;
          case EVENT_DROP_TABLE:
            log.info("Received drop table event on path : {}", path);
            Table table = dropTableEventHandler.dropTable(payload, id);
            log.info("Successfully marked the table dropped : {}", table.getName());
            tables.add(table);
            break;
          case EVENT_CREATE_TABLE:
            log.info("Received create table event on path : {}", path);
            try {
              Table createdTable = createTableEventHandler.createTable(path, id);
              log.info("Successfully created new table : {}", createdTable.getName());
              tables.add(createdTable);
            }
            catch(MetaFileDoesntExistException e) {
              log.error("Error occurred while handling create table event.", e);
            }
            break;
          case EVENT_ALTER_TABLE:
            log.info("Received alter table event on path : {}", path);
            Optional<Table> updatedTable = alterTableEventHandler.alterTable(payload, id);
            updatedTable.ifPresent(tables::add);
            break;

          case EVENT_ALTER_PARTITION: // Handles ADD_PARTITION events too
            log.info("Received patrition event on path : {}", path);
            TablePartitionInfo tablePartitionInfo = partitionEventHandler.upsertPartitionInfo(payload, id);
            log.info("Successfully upserted partition : {} in table with id : {}", tablePartitionInfo.getPartitionName(), tablePartitionInfo.getTableId());
            break;
          case EVENT_DROP_PARTITION:
            log.info("Received drop patrition event on path : {}", path);
            Collection<TablePartitionInfo> partitionInfos = partitionEventHandler.dropPartitionInfo(payload, id);
            String tableNames = partitionInfos.stream().map((info) -> info.getTableId().toString()).collect(Collectors.joining( "," ));
            log.info("Partition drop happened in table(s) with ids : {}", tableNames);
            break;
          default:
            break;
        }
      }

      log.info("Successfully processsed incremental dump : {} and saved replication info : {}", incrementalDump);

      int delay = eventProcessingConfig.getAsInteger(Constants.META_INFO_SYNC_SERVICE_DELAY_MILLIS,
        Constants.DEFAULT_META_INFO_SYNC_SERVICE_DELAY_MILLIS);
      dbReplicationRepositoryProvider.get().finishReplProcessing(
          dbReplicationEntity.getDatabaseName(), delay, incrementalDump.getLastReplicationId());

      return new DatabasesAndTables(databases, tables, dbReplicationEntity);
    } catch (Exception e) {
      log.error("Error occurred while reading dump metadata {} : ", incrementalDump, e);
      throw new MetaDataUpdationException("error occurred while updating warehouse for dump " + incrementalDump);
    } finally {
      deleteDump(incrementalDump);
    }
  }
}
