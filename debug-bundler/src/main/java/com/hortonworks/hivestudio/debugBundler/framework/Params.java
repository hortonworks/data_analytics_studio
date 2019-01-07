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

import com.google.common.base.Strings;
import com.hortonworks.hivestudio.debugBundler.helpers.AMArtifactsHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Params {
  private String appType;
  private String remoteUser;
  private String domainId;

  private boolean enableLogExtraction = true;

  // Tez information.
  private String tezDagId;
  private String tezAmAppId;

  private final AppLogs tezAmLogs = new AppLogs();
  private final AppLogs tezTaskLogs = new AppLogs();

  // Slider AM info.
  private String sliderAppId;
  private final AppLogs sliderAmLogs = new AppLogs();
  private Set<String> sliderInstanceUrls;

  // Hive information.
  private String hiveQueryId;

  // Start and End time of query/dag.
  private long startTime = 0;
  private long endTime = Long.MAX_VALUE;

  public static class AppLogs {
    // Node -> Container -> log
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, List<ContainerLogInfo>>>
        appLogs = new ConcurrentHashMap<>();

    private boolean finishedContainers;
    private boolean finishedLogs;

    public int logSize() {
      return appLogs.size();
    }

    public void addLog(String nodeId, String containerId, List<ContainerLogInfo> logs)  {
      if (!appLogs.containsKey(nodeId)) {
        appLogs.putIfAbsent(nodeId, new ConcurrentHashMap<String, List<ContainerLogInfo>>());
      }
      appLogs.get(nodeId).put(containerId, logs);
    }

    public void addContainer(String nodeId, String containerId) {
      addLog(nodeId, containerId, Collections.<ContainerLogInfo>emptyList());
    }

    public boolean isFinishedContainers() {
      return finishedContainers;
    }

    public void finishContainers() {
      this.finishedContainers = true;
    }

    public boolean isFinishedLogs() {
      return finishedLogs;
    }

    public void finishLogs() {
      this.finishedLogs = true;
    }

    public List<Artifact> getLogListArtifacts(AMArtifactsHelper helper, String name) {
      List<Artifact> artifacts = new ArrayList<>();
      for (Entry<String, ConcurrentHashMap<String, List<ContainerLogInfo>>> entry :
          appLogs.entrySet()) {
        String nodeId = entry.getKey();
        for (String containerId : entry.getValue().keySet()) {
          artifacts.add(helper.getLogListArtifact(name + "/" + containerId + ".logs.json",
              containerId, nodeId));
        }
      }
      return artifacts;
    }

    public List<Artifact> getLogArtifacts(AMArtifactsHelper helper, String name) {
      List<Artifact> artifacts = new ArrayList<>();
      for (Entry<String, ConcurrentHashMap<String, List<ContainerLogInfo>>> entry :
          appLogs.entrySet()) {
        String nodeId = entry.getKey();
        for (Entry<String, List<ContainerLogInfo>> e : entry.getValue().entrySet()) {
          String containerId = e.getKey();
          for (ContainerLogInfo log: e.getValue()) {
            artifacts.add(helper.getLogArtifact(name + "/" + containerId + "/" + log.fileName,
                containerId, log.fileName, nodeId));
          }
        }
      }
      return artifacts;
    }
  }

  public static class ContainerLogInfo {
    public ContainerLogInfo() {}
    public ContainerLogInfo(String fileName, long fileSize, String lastModifiedTime) {
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.lastModifiedTime = lastModifiedTime;
    }
    public String fileName;
    public long fileSize;
    public String lastModifiedTime;
  }

  public static class ContainerLogsInfo {
    public List<ContainerLogInfo> containerLogInfo;
    public String containerId;
    public String nodeId;
  }

  public String getAppType() {
    return appType;
  }

  public void setAppType(String appType) {
    this.appType = appType;
  }

  public Boolean getUsesTez() {
    if(!Strings.isNullOrEmpty(appType)) {
      switch(appType) {
        case "TEZ":
        case "LLAP":
          return true;
      }
    }
    return false;
  }

  public String getRemoteUser() {
    return remoteUser;
  }

  public void setRemoteUser(String remoteUser) {
    this.remoteUser = remoteUser;
  }

  public String getDomainId() {
    return domainId;
  }

  public void setDomainId(String domainId) {
    this.domainId = domainId;
  }

  public boolean getEnableLogExtraction() {
    return enableLogExtraction;
  }

  public void setEnableLogExtraction(boolean enableLogExtraction) {
    this.enableLogExtraction = enableLogExtraction;
  }

  public String getTezDagId() {
    return tezDagId;
  }

  public void setTezDagId(String tezDagId) {
    this.tezDagId = tezDagId;
  }

  public String getTezAmAppId() {
    return tezAmAppId;
  }

  public void setTezAmAppId(String tezAmAppId) {
    this.tezAmAppId = tezAmAppId;
  }


  public AppLogs getTezAmLogs() {
    return tezAmLogs;
  }

  public AppLogs getTezTaskLogs() {
    return tezTaskLogs;
  }

  public String getSliderAppId() {
    return sliderAppId;
  }

  public void setSliderAppId(String sliderAppId) {
    this.sliderAppId = sliderAppId;
  }

  public AppLogs getSliderAmLogs() {
    return sliderAmLogs;
  }

  public Set<String> getSliderInstanceUrls() {
    return sliderInstanceUrls;
  }

  public void setSliderInstanceUrls(Set<String> sliderInstanceUrls) {
    this.sliderInstanceUrls = sliderInstanceUrls;
  }

  public String getHiveQueryId() {
    return hiveQueryId;
  }

  public void setHiveQueryId(String hiveQueryId) {
    this.hiveQueryId = hiveQueryId;
  }

  public long getStartTime() {
    return startTime;
  }

  public void updateStartTime(long startTime) {
    if (this.startTime == 0 || startTime < this.startTime) {
      this.startTime = startTime;
    }
  }

  public long getEndTime() {
    return endTime;
  }

  public void updateEndTime(long endTime) {
    if (this.endTime == Long.MAX_VALUE || endTime > this.endTime) {
      this.endTime = endTime;
    }
  }

  public boolean shouldIncludeArtifact(long startTime, long endTime) {
    if (endTime == 0) {
      endTime = Long.MAX_VALUE;
    }
    // overlap is true if one of them started when other was running.
    return (this.startTime <= startTime && startTime <= this.endTime) ||
        (startTime <= this.startTime && this.startTime <= endTime);
  }
}
