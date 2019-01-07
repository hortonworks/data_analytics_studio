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

package com.hortonworks.hivestudio.debugBundler.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.debugBundler.entities.history.DAGEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.TaskAttemptEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.TaskEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.VertexEntityType;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.HistoryEventsArtifact;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.query.services.DagInfoService;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class TezHDFSArtifactsTest {

  @Mock private DagInfoService dagInfoService;
  @Mock private ObjectMapper objectMapper;

  private Configuration configuration = new Configuration();

  private DAGEntityType dagEntityType;
  private VertexEntityType vertexEntityType;
  private TaskEntityType taskEntityType;
  private TaskAttemptEntityType taskAttemptEntityType;

  TezHDFSArtifacts tezHDFSArtifacts;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    dagEntityType = new DAGEntityType(objectMapper, configuration);
    vertexEntityType = new VertexEntityType(objectMapper, configuration);
    taskEntityType = new TaskEntityType(objectMapper, configuration);
    taskAttemptEntityType = new TaskAttemptEntityType(objectMapper, configuration);

    tezHDFSArtifacts = new TezHDFSArtifacts(dagInfoService, dagEntityType, vertexEntityType, taskEntityType, taskAttemptEntityType);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void getArtifacts() throws Exception {
    Params params = new Params();

    Mockito.when(dagInfoService.getOneByDagId(Mockito.any())).thenReturn(Optional.empty());
    List<Artifact> artifacts = tezHDFSArtifacts.getArtifacts(params);
    Assert.assertEquals("Invalid number of artifacts", 0, artifacts.size());

    DagInfo dagInfo = new DagInfo();
    dagInfo.setSourceFile("/test");
    Mockito.when(dagInfoService.getOneByDagId(Mockito.any())).thenReturn(Optional.of(dagInfo));
    artifacts = tezHDFSArtifacts.getArtifacts(params);
    Assert.assertEquals("Invalid number of artifacts", 4, artifacts.size());
  }

  @Test
  public void updateParams() throws Exception {
    String nodeId = "ctr-e138-1518143905142-420783-01-000006.hwx.site:25454";

    Params params = new Params();

    Artifact taskAttemptArtifact = new HistoryEventsArtifact(taskAttemptEntityType, new org.apache.hadoop.fs.Path("/test"));

    ObjectNode objectNode = (ObjectNode) new ObjectMapper().readTree("{\"task_attempt\":[]}");
    Mockito.when(objectMapper.readTree(Mockito.any(InputStream.class))).thenReturn(objectNode);
    tezHDFSArtifacts.updateParams(params, taskAttemptArtifact, Paths.get("/"));
    Assert.assertTrue("Finish not set", params.getTezTaskLogs().isFinishedContainers());
    Assert.assertEquals("Log was added", 0, params.getTezTaskLogs().logSize());

    params = new Params();
    objectNode = (ObjectNode) new ObjectMapper().readTree("{\"" +
      "task_attempt\":[{\"" +
        "events\":[{\"" +
          "completedLogsURL\":\"http://ctr-e138-1518143905142-420783-01-000003.hwx.site:8188/ws/v1/applicationhistory/containers/container_1532921752698_0002_01_000002/logs/hive_20180730071634_84e7218c-c6dd-439a-a153-607277a143b3-dag_1532921752698_0004_43.log.done?nm.id=" + nodeId + "\"" +
        "}]" +
      "}]}");

    Mockito.when(objectMapper.readTree(Mockito.any(InputStream.class))).thenReturn(objectNode);
    tezHDFSArtifacts.updateParams(params, taskAttemptArtifact, Paths.get("/"));
    Assert.assertTrue("Finish not set", params.getTezTaskLogs().isFinishedContainers());
    Assert.assertEquals("Log was not added", 1, params.getTezTaskLogs().logSize());
  }

}