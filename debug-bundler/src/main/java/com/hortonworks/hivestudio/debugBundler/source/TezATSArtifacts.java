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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.debugBundler.helpers.ATSArtifactHelper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TezATSArtifacts implements ArtifactSource {

  private final ATSArtifactHelper helper;
  private final ObjectMapper objectMapper;
  private static final Pattern logsPattern = Pattern.compile(
      "^.*applicationhistory/containers/(.*?)/logs.*\\?nm.id=(.+:[\\d+]+).*$");

  @Inject
  public TezATSArtifacts(ATSArtifactHelper helper) {
    this.helper = helper;
    this.objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    String dagId = params.getTezDagId();
    return ImmutableList.of(
        helper.getEntityArtifact("ATS/TEZ_DAG", "TEZ_DAG_ID", dagId),
        helper.getEntityArtifact("ATS/TEZ_DAG_EXTRA_INFO", "TEZ_DAG_EXTRA_INFO", dagId),
        helper.getChildEntityArtifact("ATS/TEZ_VERTEX", "TEZ_VERTEX_ID", "TEZ_DAG_ID", dagId),
        helper.getChildEntityArtifact("ATS/TEZ_TASK", "TEZ_TASK_ID", "TEZ_DAG_ID", dagId),
        helper.getChildEntityArtifact("ATS/TEZ_TASK_ATTEMPT", "TEZ_TASK_ATTEMPT_ID",
            "TEZ_DAG_ID", dagId));
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path)
      throws ArtifactDownloadException {
    try {
      if (artifact.getName().equals("ATS/TEZ_DAG")) {
        extractDagData(params, path);
      }
      if (artifact.getName().equals("ATS/TEZ_TASK_ATTEMPT")) {
        extractTaskContainers(params, path);
      }
    } catch (IOException e) {
      throw new ArtifactDownloadException(e);
    }
  }

  private void extractTaskContainers(Params params, Path path) throws IOException {
    Params.AppLogs appLogs = params.getTezTaskLogs();
    if (appLogs.isFinishedContainers()) {
      return;
    }
    InputStream stream = Files.newInputStream(path);
    JsonNode node = objectMapper.readTree(stream);
    if (node == null || !node.isObject()) {
      return;
    }
    node = node.get("entities");
    if (node == null || !node.isArray()) {
      return;
    }
    for (int i = 0; i < node.size(); ++i) {
      JsonNode entity = node.get(i).path("otherinfo");
      // TODO(hjp): Check if there is inProgressLogsURL and try this out.
      String logsUrl = entity.path("completedLogsURL").asText();
      if (logsUrl == null || logsUrl.isEmpty()) {
        continue;
      }
      Matcher matcher = logsPattern.matcher(logsUrl);
      if (matcher.matches()) {
        String containerId = matcher.group(1);
        String nodeId = matcher.group(2);
        appLogs.addContainer(nodeId, containerId);
      } else {
        String containerId = entity.path("containerId").asText();
        String nodeId = entity.path("nodeId").asText();
        if (!containerId.isEmpty() && !nodeId.isEmpty()) {
          appLogs.addContainer(nodeId, containerId);
        }
      }
    }
    appLogs.finishContainers();
  }

  private void extractDagData(Params params, Path path) throws IOException {
    InputStream stream = Files.newInputStream(path);
    JsonNode node = objectMapper.readTree(stream);
    if (node == null) {
      return;
    }
    if (params.getDomainId() == null && params.isAclsEnabled()) {
      String domain = node.path("domain").asText();
      if (domain != null && !domain.isEmpty()) {
        params.setDomainId(domain);
      }
    }
    JsonNode other = node.get("otherinfo");
    if (other == null) {
      return;
    }
    // Get and update dag id/hive query id.
    if (params.getTezAmAppId() == null) {
      String appId = other.path("applicationId").asText();
      if (appId != null && !appId.isEmpty()) {
        params.setTezAmAppId(appId);
      }
    }
    if (params.getHiveQueryId() == null) {
      String callerType = other.path("callerType").asText();
      String callerId = other.path("callerId").asText();
      if (!callerType.isEmpty() && !callerId.isEmpty() && callerType.equals("HIVE_QUERY_ID")) {
        params.setHiveQueryId(callerId);
      }
    }
    ATSArtifactHelper.ATSLog log = objectMapper.treeToValue(node, ATSArtifactHelper.ATSLog.class);
    for (ATSArtifactHelper.ATSEvent event : log.events) {
      if (event.eventtype != null) {
        if (event.eventtype.equals("DAG_SUBMITTED")) {
          params.updateStartTime(event.timestamp);
        } else if (event.eventtype.equals("DAG_FINISHED")) {
          params.updateEndTime(event.timestamp);
        }
      }
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezDagId() != null;
  }
}
