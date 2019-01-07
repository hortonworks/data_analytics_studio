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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.debugBundler.entities.history.DAGEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.TaskAttemptEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.TaskEntityType;
import com.hortonworks.hivestudio.debugBundler.entities.history.VertexEntityType;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.HistoryEventsArtifact;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.query.services.DagInfoService;

public class TezHDFSArtifacts implements ArtifactSource {

  private static final Pattern LOGS_URL_PATTERN = Pattern.compile(
    "^.*applicationhistory/containers/(.*?)/logs.*\\?nm.id=(.+:[\\d+]+).*$");

  private final DagInfoService dagInfoService;

  private final DAGEntityType dagEntityType;
  private final VertexEntityType vertexEntityType;
  private final TaskEntityType taskEntityType;
  private final TaskAttemptEntityType taskAttemptEntityType;

  @Inject
  public TezHDFSArtifacts(DagInfoService dagInfoService,
                          DAGEntityType dagEntityType, VertexEntityType vertexEntityType,
                          TaskEntityType taskEntityType, TaskAttemptEntityType taskAttemptEntityType) {
    this.dagInfoService = dagInfoService;

    this.dagEntityType = dagEntityType;
    this.vertexEntityType = vertexEntityType;
    this.taskEntityType = taskEntityType;
    this.taskAttemptEntityType = taskAttemptEntityType;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    Optional<DagInfo> dagInfo = dagInfoService.getOneByDagId(params.getTezDagId());

    if(!dagInfo.isPresent()) {
      return ImmutableList.of();
    }

    org.apache.hadoop.fs.Path sourceFile = new org.apache.hadoop.fs.Path(dagInfo.get().getSourceFile());

    Artifact dagArtifact = new HistoryEventsArtifact(dagEntityType, sourceFile);
    Artifact vertexArtifact = new HistoryEventsArtifact(vertexEntityType, sourceFile);
    Artifact taskArtifact = new HistoryEventsArtifact(taskEntityType, sourceFile);
    Artifact taskAttemptArtifact = new HistoryEventsArtifact(taskAttemptEntityType, sourceFile);

    return ImmutableList.of(dagArtifact, vertexArtifact, taskArtifact, taskAttemptArtifact);
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws ArtifactDownloadException {
    try {

      if(artifact.getName().startsWith(taskAttemptEntityType.getPath())) {

        Params.AppLogs appLogs = params.getTezTaskLogs();
        if (appLogs.isFinishedContainers()) {
          return;
        }
        InputStream stream = Files.newInputStream(path);
        JsonNode node = taskAttemptEntityType.getObjectMapper().readTree(stream);
        if (node == null) {
          return;
        }
        node = node.get(taskAttemptEntityType.getName().toLowerCase());
        if (node == null || !node.isArray()) {
          return;
        }

        for (JsonNode element : node) {

          JsonNode taskAttemptStartedEvent = element.path("events").get(0);

          String logsUrl = taskAttemptStartedEvent.path("completedLogsURL").asText();
          if (logsUrl == null || logsUrl.isEmpty()) {
            continue;
          }

          Matcher matcher = LOGS_URL_PATTERN.matcher(logsUrl);
          if (matcher.matches()) {
            String containerId = matcher.group(1);
            String nodeId = matcher.group(2);
            appLogs.addContainer(nodeId, containerId);
          } else {
            String containerId = taskAttemptStartedEvent.path("containerId").asText();
            String nodeId = taskAttemptStartedEvent.path("nodeId").asText();
            if (!containerId.isEmpty() && !nodeId.isEmpty()) {
              appLogs.addContainer(nodeId, containerId);
            }
          }

        }

        appLogs.finishContainers();
      }

    } catch (IOException e) {
      throw new ArtifactDownloadException(e);
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezDagId() != null;
  }

}
