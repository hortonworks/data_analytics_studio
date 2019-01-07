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

import com.google.inject.Inject;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.debugBundler.helpers.AMArtifactsHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public class TezTasksLogsListArtifacts implements ArtifactSource {
  private final AMArtifactsHelper helper;

  @Inject
  public TezTasksLogsListArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getEnableLogExtraction() && params.getUsesTez() &&
      params.getTezTaskLogs().isFinishedContainers();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return params.getTezTaskLogs().getLogListArtifacts(helper, "TEZ_TASKS/LOGS");
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path)
      throws ArtifactDownloadException {
    List<Params.ContainerLogsInfo> logsInfoList;
    try {
      logsInfoList = helper.parseContainerLogs(path);
    } catch (IOException e) {
      throw new ArtifactDownloadException(e);
    }
    if (logsInfoList != null) {
      for (Params.ContainerLogsInfo logsInfo : logsInfoList) {
        filterLogs(logsInfo.containerLogInfo, params);
        params.getTezTaskLogs().addLog(
            logsInfo.nodeId, logsInfo.containerId, logsInfo.containerLogInfo);
      }
      // This is not correct, but we have no way to tell all the logs have downloaded
      params.getTezTaskLogs().finishLogs();
    }
  }

  private void filterLogs(List<Params.ContainerLogInfo> containerLogInfo, Params params) {
    String syslogPrefix = "syslog_attempt_" + params.getTezDagId().substring(4) + "_";
    Iterator<Params.ContainerLogInfo> iter = containerLogInfo.iterator();
    while (iter.hasNext()) {
      String fileName = iter.next().fileName;
      if (fileName.startsWith("syslog_attempt_") && !fileName.startsWith(syslogPrefix)) {
        iter.remove();
      }
    }
  }

}
