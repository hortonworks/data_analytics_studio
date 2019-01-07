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

import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.ALL_DB_STAR;
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.CONSTRAINTS;
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.FUNCTIONS;
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.METADATA;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import javax.inject.Provider;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaDataUpdationException;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaFileDoesntExistException;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaInfoUtils;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.DatabaseComparator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetaEventHandler {
  protected static final List<String> ignoredFiles = Arrays.asList(METADATA, CONSTRAINTS, FUNCTIONS);
  protected final Provider<TableRepository> tableRepository;
  protected final MetaInfoUtils metaInfoUtils;
  protected final Provider<ColumnRepository> columnRepository;
  protected final Provider<DatabaseRepository> databaseRepository;
  protected DatabaseComparator databaseComparator;
  protected Provider<DBReplicationRepository> dbReplicationRepository;
  protected HdfsApi hdfsApi;

  public MetaEventHandler(Provider<TableRepository> tableRepository,
                          MetaInfoUtils metaInfoUtils, Provider<ColumnRepository> columnRepository,
                          Provider<DatabaseRepository> databaseRepository, DatabaseComparator databaseComparator,
                          Provider<DBReplicationRepository> dbReplicationRepository, HdfsApi hdfsApi) {
    this.tableRepository = tableRepository;
    this.metaInfoUtils = metaInfoUtils;
    this.columnRepository = columnRepository;
    this.databaseRepository = databaseRepository;
    this.databaseComparator = databaseComparator;
    this.dbReplicationRepository = dbReplicationRepository;
    this.hdfsApi = hdfsApi;
  }

  protected Database createDatabase(org.apache.hadoop.hive.metastore.api.Database database) {
    Database hsDatabase = databaseComparator.createFromHiveDatabase(database);
    hsDatabase.setDropped(Boolean.FALSE);
    hsDatabase.setCreateTime(new Date());
    hsDatabase.setCreationSource(CreationSource.REPLICATION);
    databaseRepository.get().upsert(hsDatabase);
    return hsDatabase;
  }



  protected Table createTableInternal(org.apache.hadoop.hive.metastore.api.Table hiveTable, Database database) {
    ColumnRepository columnRepo = columnRepository.get();
    log.info("Creating new table {}.{}", database.getName(), hiveTable.getTableName());
    Date now = new Date();
    Pair<Table, Collection<Column>> tableSpec = metaInfoUtils.convertHiveTableToHSTable(hiveTable, database, now);
    Table savedTable = tableRepository.get().upsert(tableSpec.getFirst());
    List<Column> columns = columnRepo.getAllForTableNotDropped(tableSpec.getFirst().getId());

    // Update old columns
    columns.forEach(column -> {
      column.setLastUpdatedAt(now);
      column.setDropped(Boolean.FALSE);
      columnRepo.upsert(column);
    });
    // Insert new columns
    tableSpec.getSecond().forEach(column -> {
      column.setTableId(savedTable.getId());
      column.setLastUpdatedAt(now);
      column.setDropped(Boolean.FALSE);
      columnRepo.upsert(column);
    });

    return savedTable;
  }

  protected Table createTableInternal(org.apache.hadoop.hive.metastore.api.Table table) {
    Database database = databaseRepository.get().getByDatabaseNameAndNotDropped(table.getDbName());
    return createTableInternal(table, database);
  }


  @DASTransaction
  protected <T> T persistReplicationEvent(Supplier<T> block, int replicationId) {
    T t = block.get();
    // save the last replication id
    dbReplicationRepository.get().setReplicationId(ALL_DB_STAR, String.valueOf(replicationId));
    return t;
  }

  @VisibleForTesting
  protected MetaData parseMetaData(Path path) {
    log.info("Reading the table meta data in path : {}", path);
    try {
      Path metaFilePath = new Path(path, METADATA);
      if(!hdfsApi.getFileSystem().exists(metaFilePath)) {
        throw new MetaFileDoesntExistException(metaFilePath);
      }
      return EximUtil.readMetaData(hdfsApi.getFileSystem(), metaFilePath);
    } catch (IOException | SemanticException e) {
      log.error("Error occurred while reading the metadata for table : {}", path);
      throw new MetaDataUpdationException("Error occurred while fetching metadata of table from dump.", e);
    }
  }
}
