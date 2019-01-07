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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TablePartitionInfoRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.eventProcessor.meta.MetaInfoUtils;
import com.hortonworks.hivestudio.eventProcessor.meta.diff.DatabaseComparator;

public class PartitionEventHandler extends MetaEventHandler {

  private Provider<TablePartitionInfoRepository> tablePartitionInfoRepositoryProvider;
  private MessageDeserializer deserializer = MessageFactory.getInstance().getDeserializer();
  private ObjectMapper objectMapper = new ObjectMapper();

  @Inject
  public PartitionEventHandler(Provider<TableRepository> tableRepository,
                               MetaInfoUtils metaInfoUtils, Provider<ColumnRepository> columnRepository,
                               Provider<DatabaseRepository> databaseRepository, DatabaseComparator databaseComparator,
                               Provider<DBReplicationRepository> dbReplicationRepository,
                               Provider<TablePartitionInfoRepository> tablePartitionInfoRepositoryProvider,
                               HdfsApi hdfsApi) {
    super(tableRepository,
      metaInfoUtils, columnRepository,
      databaseRepository, databaseComparator, dbReplicationRepository, hdfsApi);

    this.tablePartitionInfoRepositoryProvider = tablePartitionInfoRepositoryProvider;
  }

  @VisibleForTesting
  String generatePartitionName(LinkedHashMap<String, String> partitionMap) {
    ArrayList<String> cols = new ArrayList<>();
    for (Map.Entry<String, String> partition : partitionMap.entrySet()) {
      cols.add("/");
      cols.add(partition.getKey());
      cols.add("=");
      cols.add(partition.getValue());
    }
    return String.join("", cols);
  }

  @VisibleForTesting
  TablePartitionInfo upsertTablePartitionInfo(Partition partition, LinkedHashMap<String, String> partitionDetails) {
    Table originalTable = tableRepository.get().getByDBNameTableNameAndNotDropped(partition.getDbName(), partition.getTableName());

    String partitionName = generatePartitionName(partitionDetails);

    TablePartitionInfo info = new TablePartitionInfo();
    info.setTableId(originalTable.getId());
    info.setPartitionName(partitionName);
    info.setDetails(objectMapper.valueToTree(partitionDetails));

    Map<String, String> params = partition.getParameters();
    if(params.containsKey("numFiles")) {
      info.setNumFiles(Integer.parseInt(params.get("numFiles")));
    }
    if(params.containsKey("numRows")) {
      info.setNumRows(Integer.parseInt(params.get("numRows")));
    }
    if(params.containsKey("rawDataSize")) {
      info.setRawDataSize(Long.parseLong(params.get("rawDataSize")));
    }

    tablePartitionInfoRepositoryProvider.get().upsert(info);

    return info;
  }

  @VisibleForTesting
  TablePartitionInfo dropTablePartitionInfo(Table table, String partitionName) {
    TablePartitionInfo partitionInfo = tablePartitionInfoRepositoryProvider.get().getOne(table.getId(), partitionName);
    if(partitionInfo != null) {
      tablePartitionInfoRepositoryProvider.get().delete(partitionInfo.getId());
    }
    return partitionInfo;
  }

  public TablePartitionInfo upsertPartitionInfo(String payload, Integer id) throws Exception {
    AlterPartitionMessage alterPartitionMessage = deserializer.getAlterPartitionMessage(payload);
    Partition ptnObjAfter = alterPartitionMessage.getPtnObjAfter();
    return persistReplicationEvent(() -> upsertTablePartitionInfo(ptnObjAfter, (LinkedHashMap<String, String>) alterPartitionMessage.getKeyValues()), id);
  }

  public Collection<TablePartitionInfo> dropPartitionInfo(String payload, Integer id) throws Exception {
    DropPartitionMessage dropPartitionMessage = deserializer.getDropPartitionMessage(payload);

    ArrayList<TablePartitionInfo> partitionInfos = new ArrayList<>();

    Table parentTable = tableRepository.get().getByDBNameTableNameAndNotDropped(dropPartitionMessage.getDB(), dropPartitionMessage.getTable());
    for (Map<String, String> partition : dropPartitionMessage.getPartitions()) {
      String dropPartitionName = generatePartitionName((LinkedHashMap<String, String>) partition);
      partitionInfos.add(persistReplicationEvent(() -> dropTablePartitionInfo(parentTable, dropPartitionName), id));
    }

    return partitionInfos;
  }

}
