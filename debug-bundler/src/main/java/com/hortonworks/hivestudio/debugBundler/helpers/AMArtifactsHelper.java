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

package com.hortonworks.hivestudio.debugBundler.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.HttpArtifact;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

@Singleton
public class AMArtifactsHelper {

  private static final String RM_WS_PREFIX = "/ws/v1/cluster";
  private static final String AHS_WS_PREFIX = "/ws/v1/applicationhistory";

  private final String rmAddress;
  private final String ahsAddress;
  private final ObjectMapper objectMapper;
  private final YarnConfiguration conf;

  @Inject
  public AMArtifactsHelper(YarnConfiguration conf) {
    this.conf = conf;
    this.objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

    String yarnHTTPPolicy = conf.get(YarnConfiguration.YARN_HTTP_POLICY_KEY,
      YarnConfiguration.YARN_HTTP_POLICY_DEFAULT);

    if (HttpConfig.Policy.HTTPS_ONLY == HttpConfig.Policy.fromString(yarnHTTPPolicy)) {
      rmAddress = "https://" + conf.get(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
        YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS);
      ahsAddress = "https://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS);
    } else {
      rmAddress = "http://" + conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
      ahsAddress = "http://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS);
    }
  }

  private String getConfig(Configuration conf, org.apache.hadoop.conf.Configuration hadoopConf, String key, String defaultValue) {
    Optional<String> valueOptional = conf.get(key);
    if(valueOptional.isPresent()) {
      return valueOptional.get();
    }
    return hadoopConf.get(key, defaultValue);
  }

  public Artifact getAMInfoArtifact(String name, String appId) {
    String attemptsUrl = rmAddress + RM_WS_PREFIX + "/apps/" + appId + "/appattempts";
    return new HttpArtifact(conf, name, attemptsUrl, false);
  }

  public Artifact getLogListArtifact(String name, String containerId, String nodeId) {
    String logsListUrl = ahsAddress + AHS_WS_PREFIX + "/containers/" + containerId + "/logs";
    if (nodeId != null) {
      logsListUrl += "?nm.id=" + nodeId;
    }
    return new HttpArtifact(conf, name, logsListUrl, false);
  }

  public Artifact getLogArtifact(String name, String containerId, String logFile, String nodeId) {
    String logUrl = ahsAddress + AHS_WS_PREFIX + "/containers/" + containerId + "/logs/" + logFile;
    if (nodeId != null) {
      logUrl += "?nm.id=" + nodeId;
    }
    return new HttpArtifact(conf, name, logUrl, false);
  }

  public static class ContainerLogs {
    public List<Params.ContainerLogsInfo> containerLogsInfo;
  }
  public List<Params.ContainerLogsInfo> parseContainerLogs(Path path) throws IOException {
    TypeReference<List<Params.ContainerLogsInfo>> typeRef = new TypeReference<List<Params.ContainerLogsInfo>>(){};
    // The api return with containerLogsInfo or without it, hence trying both formats.
    try {
      return objectMapper.readValue(Files.newInputStream(path), ContainerLogs.class).containerLogsInfo;
    } catch (JsonProcessingException e) {
      return objectMapper.readValue(Files.newInputStream(path), typeRef);
    }
  }
}
