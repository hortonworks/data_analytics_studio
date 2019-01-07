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
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.CONSTRAINTS;
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.DUMPMETADATA;
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.FUNCTIONS;
import static com.hortonworks.hivestudio.eventProcessor.configuration.Constants.METADATA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpInfo;
import com.hortonworks.hivestudio.common.entities.DBReplicationEntity;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.eventProcessor.configuration.EventProcessingConfig;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.ColumnComparatorForDiff;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.TableComparatorForDiff;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.AlterDatabaseEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.AlterTableEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.BootstrapEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.CreateDatabaseEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.CreateTableEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.DropDatabaseEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.DropTableEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.PartitionEventHandler;
import com.hortonworks.hivestudio.eventProcessor.meta.handlers.RenameTableEventHandler;

public class MetaInfoUpdaterTest {

  TableComparatorForDiff tableComparatorForDiff;
  @Mock HiveRepl hiveRepl;
  @Mock HdfsApi hdfsApi;
  @Mock Configuration hadoopConfiguration;
  @Mock HiveConfiguration hiveConfiguration;
  @Mock MetaInfoUtils metaInfoUtils;
  @Mock MetaInfoUpdater metaInfoUpdater;
  @Mock HiveConf hiveConf;
  @Mock EventProcessingConfig eventProcessingConfig;

  @Mock Provider<DBReplicationRepository> dbReplicationRepositoryProvider;
  @Mock Provider<ColumnRepository> columnRepositoryProvider;
  @Mock DBReplicationRepository dbReplicationRepository;

  @Mock DropDatabaseEventHandler dropDatabaseEventHandler = null;
  @Mock AlterDatabaseEventHandler alterDatabaseEventHandler = null;
  @Mock CreateTableEventHandler createTableEventHandler = null;
  @Mock AlterTableEventHandler alterTableEventHandler = null;
  @Mock CreateDatabaseEventHandler createDatabaseEventHandler = null;
  @Mock RenameTableEventHandler renameTableEventHandler = null;
  @Mock DropTableEventHandler dropTableEventHandler = null;
  @Mock BootstrapEventHandler bootstrapEventHandler = null;
  @Mock PartitionEventHandler partitionEventHandler = null;


  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    tableComparatorForDiff = new TableComparatorForDiff(new ColumnComparatorForDiff(), columnRepositoryProvider);

    when(metaInfoUtils.createHiveConfs(any())).thenReturn(hiveConf);
    when(hiveConfiguration.keySet()).thenReturn(Collections.emptySet());
    when(dbReplicationRepositoryProvider.get()).thenReturn(dbReplicationRepository);

    metaInfoUpdater = new MetaInfoUpdater(dropDatabaseEventHandler, alterDatabaseEventHandler,
        hiveRepl, createTableEventHandler, hdfsApi, alterTableEventHandler,
        createDatabaseEventHandler, renameTableEventHandler, dropTableEventHandler,
        bootstrapEventHandler, partitionEventHandler, hadoopConfiguration, hiveConfiguration,
        metaInfoUtils, dbReplicationRepositoryProvider, eventProcessingConfig);
  }

  @Test
  public void bootstrapWarehouseFromDump() throws IOException, InterruptedException, SemanticException {
    String dumpPath = "some-path-to-hdfs";
    WarehouseDumpInfo dumpInfo = new WarehouseDumpInfo(dumpPath, "10");

    when(hiveRepl.getWarehouseBootstrapDump()).thenReturn(dumpInfo);

    FileStatus metaFile = mock(FileStatus.class);
    FileStatus dumpMetaFile = mock(FileStatus.class);
    FileStatus databaseFolder = mock(FileStatus.class);
    FileStatus functionFile = mock(FileStatus.class);
    FileStatus constraintsFile = mock(FileStatus.class);
    FileStatus table1FileStatus = mock(FileStatus.class);
    FileStatus table2FileStatus = mock(FileStatus.class);

    Path dumpMetaPath = new Path("/user/hive/repl/717f9403-b500-4fc8-bdc0-c0b975079d2d/" + DUMPMETADATA);
    Path databaseFolderPath = new Path("/user/hive/repl/717f9403-b500-4fc8-bdc0-c0b975079d2d/default");
    Path metaPath = new Path("/user/hive/repl/717f9403-b500-4fc8-bdc0-c0b975079d2d/default/" + METADATA);
    Path functionPath = new Path("/user/hive/repl/717f9403-b500-4fc8-bdc0-c0b975079d2d/default/" + FUNCTIONS);
    Path constraintsPath = new Path("/user/hive/repl/717f9403-b500-4fc8-bdc0-c0b975079d2d/default/" + CONSTRAINTS);
    Path table1Path = new Path("/user/hive/repl/717f9403-b500-4fc8-bdc0-c0b975079d2d/default/table1");
    Path table2Path = new Path("/user/hive/repl/717f9403-b500-4fc8-bdc0-c0b975079d2d/default/table2");

    when(metaFile.getPath()).thenReturn(metaPath);
    when(dumpMetaFile.getPath()).thenReturn(dumpMetaPath);
    when(databaseFolder.getPath()).thenReturn(databaseFolderPath);
    when(functionFile.getPath()).thenReturn(functionPath);
    when(constraintsFile.getPath()).thenReturn(constraintsPath);
    when(table1FileStatus.getPath()).thenReturn(table1Path);
    when(table2FileStatus.getPath()).thenReturn(table2Path);

    FileStatus[] warehouseDumpFiles = {dumpMetaFile, databaseFolder};
    when(hdfsApi.listdir(dumpPath)).thenReturn(warehouseDumpFiles);

    when(hdfsApi.delete(dumpPath, true)).thenReturn(true);

    metaInfoUpdater.bootstrapWarehouseFromDump();

    verify(bootstrapEventHandler, times(1)).createWarehouseFromBootstrap(warehouseDumpFiles, Integer.valueOf(dumpInfo.getLastReplicationId()));
    verify(hdfsApi, times(1)).listdir(dumpPath);
    verify(hdfsApi, times(1)).delete(dumpPath, true);
  }

  @Test
  public void bootstrapWarehouseFromDumpWithException() throws IOException, InterruptedException {
    String dumpPath = "some-path-to-hdfs";

    when(hiveRepl.getWarehouseBootstrapDump()).thenThrow(Exception.class);

    when(hdfsApi.delete(dumpPath, true)).thenReturn(true);

    try {
      metaInfoUpdater.bootstrapWarehouseFromDump();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof MetaDataUpdationException);
    }

    verify(hdfsApi, times(0)).delete(dumpPath, true);
  }

  @Test
  public void bootstrapWarehouseFromDumpWithException2() throws IOException, InterruptedException {
    String dumpPath = "some-path-to-hdfs";
    String db = "databaseName";
    WarehouseDumpInfo dumpInfo = new WarehouseDumpInfo(dumpPath, "10");

    when(hiveRepl.getWarehouseBootstrapDump()).thenReturn(dumpInfo);
    String path = new Path(dumpInfo.getDumpPath(), db).toUri().getPath();

    when(hdfsApi.listdir(path)).thenThrow(IOException.class);

    when(hdfsApi.delete(dumpPath, true)).thenReturn(true);

    try {
      metaInfoUpdater.bootstrapWarehouseFromDump();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof MetaDataUpdationException);
    }

    verify(hdfsApi, times(1)).delete(dumpPath, true);
  }

  @Test
  public void updateWarehouseFromDump() throws IOException, InterruptedException {
    String oldDumpReplId = "10";
    DBReplicationEntity dbReplicationEntity = new DBReplicationEntity(ALL_DB_STAR, oldDumpReplId, new Date(), new Date(), new Date());
    String newDumpReplId = "5";
    String newDumpPath = "path-to-new-dump";
    WarehouseDumpInfo newDumpInfo = new WarehouseDumpInfo(newDumpPath, newDumpReplId);

    when(hiveRepl.getWarehouseIncrementalDump(oldDumpReplId)).thenReturn(newDumpInfo);

    when(hdfsApi.listdir(newDumpPath)).thenThrow(IOException.class);

    when(hdfsApi.delete(newDumpPath, true)).thenReturn(true);

    try {
      metaInfoUpdater.updateWarehouseFromDump(dbReplicationEntity);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof MetaDataUpdationException);
    }

    verify(hdfsApi, times(1)).delete(newDumpPath, true);
  }
}