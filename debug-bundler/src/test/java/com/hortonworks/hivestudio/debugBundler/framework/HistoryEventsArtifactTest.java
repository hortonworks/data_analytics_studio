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
package com.hortonworks.hivestudio.debugBundler.framework;

import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.hortonworks.hivestudio.debugBundler.entities.history.DAGEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.HistoryEntity;

public class HistoryEventsArtifactTest {

  @Mock private ObjectMapper objectMapper;
  @Mock private DatePartitionedLogger<HistoryLoggerProtos.HistoryEventProto> partitionedLogger;
  @Mock private ProtoMessageReader<HistoryLoggerProtos.HistoryEventProto> reader;

  private Configuration configuration = new Configuration();

  private DAGEntityType dagEntityType;
  private Path sourceFile = new Path("/test");
  private HistoryEventsArtifact historyEventsArtifact;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    dagEntityType = new DAGEntityType(objectMapper, configuration);

    historyEventsArtifact = new HistoryEventsArtifact(dagEntityType, sourceFile);
    historyEventsArtifact.partitionedLogger = partitionedLogger;
  }

  @After
  public void tearDown() throws Exception {
  }

  private HistoryLoggerProtos.HistoryEventProto constructDagEvent(String dagId, String eventType, Map<String, String> dataMap) {
    HistoryLoggerProtos.HistoryEventProto.Builder builder = HistoryLoggerProtos.HistoryEventProto.newBuilder();
    builder.setEventType(eventType);
    builder.setDagId(dagId);

    for (Map.Entry<String, String> item : dataMap.entrySet()) {
      HistoryLoggerProtos.KVPair.Builder kvBuilder = HistoryLoggerProtos.KVPair.newBuilder();
      kvBuilder.setKey(item.getKey());
      kvBuilder.setValue(item.getValue());
      builder.addEventData(kvBuilder.build());
    }

    return builder.build();
  }

  @Test
  public void readAndCompactEntities() throws Exception {
    when(partitionedLogger.getReader(sourceFile)).thenReturn(reader);

    when(reader.readEvent())
      .thenReturn(constructDagEvent("dag_1", "DAG_SUBMITTED",
        ImmutableMap.of("d1_k1", "d1_v1", "d1_k2", "d1_v2")))
      .thenReturn(constructDagEvent("dag_2", "DAG_SUBMITTED",
        ImmutableMap.of("d2_k1", "d2_v1", "d2_k2", "d2_v2")))
      .thenReturn(constructDagEvent("dag_1", "DAG_INITIALIZED",
        ImmutableMap.of("d1_k3", "d1_v3", "d1_k4", "d1_v4")))
      .thenReturn(null);

    Collection<HistoryEntity> historyEntities = historyEventsArtifact.readAndCompactEntities();
    Iterator<HistoryEntity> iterator = historyEntities.iterator();

    Assert.assertEquals("Invalid number of entities", 2, historyEntities.size());

    HistoryEntity entity = iterator.next();
    Assert.assertEquals("Invalid entity id", "dag_1", entity.getEntityId());
    Assert.assertEquals("Invalid entity type", "DAG", entity.getEntityTypeName());
    Assert.assertEquals("Invalid event count", 2, entity.getEvents().size());

    entity = iterator.next();
    Assert.assertEquals("Invalid entity", "dag_2", entity.getEntityId());
    Assert.assertEquals("Invalid entity type", "DAG", entity.getEntityTypeName());
    Assert.assertEquals("Invalid event count", 1, entity.getEvents().size());
  }

}