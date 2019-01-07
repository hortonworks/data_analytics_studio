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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class HiveATSArtifacts implements ArtifactSource {

  private final ATSArtifactHelper helper;
  private final ObjectMapper objectMapper;

  @Inject
  public HiveATSArtifacts(ATSArtifactHelper helper) {
    this.helper = helper;
    this.objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return ImmutableList.of(helper.getEntityArtifact("ATS/HIVE_QUERY", "HIVE_QUERY_ID",
        params.getHiveQueryId()));
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path)
      throws ArtifactDownloadException {
    if (artifact.getName().equals("ATS/HIVE_QUERY")) {
      JsonNode node;
      try {
        node = objectMapper.readTree(Files.newInputStream(path));
      } catch (IOException e) {
        throw new ArtifactDownloadException(e);
      }
      if (node == null) {
        return;
      }
      if (params.getDomainId() == null) {
        String domain = node.path("domain").asText();
        if (domain != null && !domain.isEmpty()) {
          params.setDomainId(domain);
        }
      }
      if (params.getAppType() == null) {
        String appType = node.path("primaryfilters").path("executionmode").path(0).asText();
        if (appType != null && !appType.isEmpty()) {
          params.setAppType(appType);
        }
      }
      JsonNode other = node.get("otherinfo");
      if (other == null) {
        return;
      }
      // Get and update dag id/hive query id.
      if (params.getTezAmAppId() == null) {
        String appId = other.path("APP_ID").asText();
        if (appId != null && !appId.isEmpty()) {
          params.setTezAmAppId(appId);
        }
      }
      if (params.getTezDagId() == null) {
        String dagId = other.path("DAG_ID").asText();
        if (dagId != null && !dagId.isEmpty()) {
          params.setTezDagId(dagId);
        }
      }
      ATSArtifactHelper.ATSLog log;
      try {
        log = objectMapper.treeToValue(node, ATSArtifactHelper.ATSLog.class);
      } catch (IOException e) {
        throw new ArtifactDownloadException(e);
      }
      for (ATSArtifactHelper.ATSEvent event : log.events) {
        if (event.eventtype != null) {
          if (event.eventtype.equals("QUERY_SUBMITTED")) {
            params.updateStartTime(event.timestamp);
          } else if (event.eventtype.equals("QUERY_COMPLETED")) {
            params.updateEndTime(event.timestamp);
          }
        }
      }
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getHiveQueryId() != null;
  }
}
