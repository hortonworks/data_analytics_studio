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

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.debugBundler.helpers.AMArtifactsHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public abstract class AMInfoArtifacts implements ArtifactSource {

  protected final AMArtifactsHelper helper;
  protected final ObjectMapper mapper;

  public AMInfoArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
  }

  public abstract String getArtifactName();

  public abstract String getAmId(Params params);

  public abstract Params.AppLogs getAMAppLogs(Params params);

  @Override
  public boolean hasRequiredParams(Params params) {
    return getAmId(params) != null;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return Collections.singletonList(
        helper.getAMInfoArtifact(getArtifactName(), getAmId(params)));
  }

  @JsonRootName("appAttempts")
  public static class AMInfo {
    public List<AppAttempt> appAttempt;
  }

  public static class AppAttempt {
    public int id;
    public long startTime;
    public long finishedTime;
    public String containerId;
    public String nodeId;
    public String appAttemptId;
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path)
      throws ArtifactDownloadException {
    Params.AppLogs amLogs = getAMAppLogs(params);
    if (amLogs.isFinishedContainers()) {
      return;
    }
    if (artifact.getName().equals(getArtifactName())) {
      AMInfo amInfo;
      try {
        amInfo = mapper.readValue(Files.newInputStream(path), AMInfo.class);
      } catch (IOException e) {
        throw new ArtifactDownloadException("Error reading value:", e);
      }
      if (amInfo != null && amInfo.appAttempt != null) {
        for (AppAttempt attempt: amInfo.appAttempt) {
          if (params.shouldIncludeArtifact(attempt.startTime, attempt.finishedTime)) {
            amLogs.addContainer(attempt.nodeId, attempt.containerId);
          }
        }
        amLogs.finishContainers();
      }
    }
  }
}
