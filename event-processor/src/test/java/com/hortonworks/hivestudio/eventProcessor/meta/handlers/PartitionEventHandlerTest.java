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

import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedHashMap;

import javax.inject.Provider;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TablePartitionInfoRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaInfoUtils;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.DatabaseComparator;

public class PartitionEventHandlerTest {

  @Mock Provider<TableRepository> tableRepositoryProvider;
  @Mock MetaInfoUtils metaInfoUtils;
  @Mock Provider<ColumnRepository> columnRepository;
  @Mock Provider<DatabaseRepository> databaseRepository;
  @Mock DatabaseComparator databaseComparator;
  @Mock Provider<DBReplicationRepository> dbReplicationRepository;
  @Mock Provider<TablePartitionInfoRepository> tablePartitionInfoRepositoryProvider;
  @Mock HdfsApi hdfsApi;

  @Mock TablePartitionInfoRepository tablePartitionInfoRepository;
  @Mock TableRepository tableRepository;

  PartitionEventHandler handler;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    handler = new PartitionEventHandler(tableRepositoryProvider, metaInfoUtils,
      columnRepository, databaseRepository, databaseComparator,
      dbReplicationRepository, tablePartitionInfoRepositoryProvider,
      hdfsApi);

    when(tableRepositoryProvider.get()).thenReturn(tableRepository);
    when(tablePartitionInfoRepositoryProvider.get()).thenReturn(tablePartitionInfoRepository);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void generatePartitionName() throws Exception {
    LinkedHashMap<String, String> partitionMap = new LinkedHashMap<>();

    Assert.assertEquals("Invalid partition name", "", handler.generatePartitionName(partitionMap));

    partitionMap.put("col1", "val11");
    Assert.assertEquals("Invalid partition name", "/col1=val11", handler.generatePartitionName(partitionMap));

    partitionMap.put("col2", "val21");
    Assert.assertEquals("Invalid partition name", "/col1=val11/col2=val21", handler.generatePartitionName(partitionMap));

    partitionMap.put("col3", "val31");
    Assert.assertEquals("Invalid partition name", "/col1=val11/col2=val21/col3=val31", handler.generatePartitionName(partitionMap));
  }

  @Test
  public void upsertTablePartitionInfo() throws Exception {
    String dbName = "db_1", tableName = "table_1";
    Integer numFiles = 1, numRows = 2;
    Long rawDataSize = 3l;

    LinkedHashMap<String, String> partitionDetails = new LinkedHashMap<>();
    partitionDetails.put("col1", "val1");
    partitionDetails.put("col2", "val2");

    com.hortonworks.hivestudio.common.entities.Table table = new com.hortonworks.hivestudio.common.entities.Table();

    HashMap<String, String> parameters = new HashMap<>();
    parameters.put("numFiles", String.valueOf(numFiles));
    parameters.put("numRows", String.valueOf(numRows));
    parameters.put("rawDataSize", String.valueOf(rawDataSize));

    Partition partition = new Partition();
    partition.setDbName(dbName);
    partition.setTableName(tableName);
    partition.setParameters(parameters);

    when(tableRepositoryProvider.get().getByDBNameTableNameAndNotDropped(dbName, tableName)).thenReturn(table);

    TablePartitionInfo info = handler.upsertTablePartitionInfo(partition, partitionDetails);

    Assert.assertEquals("Invalid table", table.getId(), info.getTableId());
    Assert.assertEquals("Invalid partition name", "/col1=val1/col2=val2", info.getPartitionName());
    Assert.assertTrue("Invalid partition details", new ObjectMapper().valueToTree(partitionDetails).equals(info.getDetails()));
    Assert.assertEquals("Invalid num files", numFiles, info.getNumFiles());
    Assert.assertEquals("Invalid num rows", numRows, info.getNumRows());
    Assert.assertEquals("Invalid data size", rawDataSize, info.getRawDataSize());
  }

  @Test
  public void dropTablePartitionInfo() throws Exception {
    String partitionName = "/col1=val11/col2=val21";
    Integer tableId = 1;

    com.hortonworks.hivestudio.common.entities.Table table = new com.hortonworks.hivestudio.common.entities.Table();
    table.setId(tableId);
    TablePartitionInfo info = new TablePartitionInfo();

    when(tablePartitionInfoRepositoryProvider.get().getOne(tableId, partitionName)).thenReturn(info);
    Assert.assertEquals("Invalid info", info, handler.dropTablePartitionInfo(table, partitionName));

    when(tablePartitionInfoRepositoryProvider.get().getOne(tableId, partitionName)).thenReturn(null);
    Assert.assertEquals("Invalid info", null, handler.dropTablePartitionInfo(table, partitionName));
  }

  @Test
  public void upsertPartitionInfo() throws Exception {
  }

  @Test
  public void dropPartitionInfo() throws Exception {
  }

}