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
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LlapDeamonLogsListArtifacts implements ArtifactSource {

  private final AMArtifactsHelper helper;

  @Inject
  public LlapDeamonLogsListArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getEnableLogExtraction() &&
      params.getAppType() != null && params.getAppType().equals("LLAP") &&
      params.getTezTaskLogs().isFinishedContainers();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return params.getTezTaskLogs().getLogListArtifacts(helper, "LLAP/LOGS");
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
    String hiveQueryId = params.getHiveQueryId();
    Iterator<Params.ContainerLogInfo> iter = containerLogInfo.iterator();
    while (iter.hasNext()) {
      String fileName = iter.next().fileName;
      if (fileName.startsWith("llap-daemon")) {
        long startTime = getLlapLogsStartTime(fileName);
        // Hourly rotation.
        if (startTime > 0 && !params.shouldIncludeArtifact(startTime, startTime + 60 * 60 * 1000)) {
          iter.remove();
        }
      } else if (fileName.startsWith("llapdaemon_history")) {
        long startTime = getLlapHistoryStartTime(fileName);
        // Daily rotation.
        long endTime = startTime + 24 * 60 * 60 * 1000;
        if (startTime > 0 && !params.shouldIncludeArtifact(startTime, endTime)) {
          iter.remove();
        }
      } else if (!fileName.startsWith(hiveQueryId) && !fileName.startsWith("gc.log")) {
        iter.remove();
      }
    }
  }

  // llapdeamon-history.log_2017-11-25_1.done
  private static final Pattern llapDaemonHistoryLogPattern =
      Pattern.compile("log_(\\d+)-(\\d+)-(\\d+)_\\d+");
  private long getLlapHistoryStartTime(String fileName) {
    long startTime = 0;
    Matcher matcher = llapDaemonHistoryLogPattern.matcher(fileName);
    if (matcher.find()) {
      int year = Integer.parseInt(matcher.group(1));
      int month = Integer.parseInt(matcher.group(2));
      int date = Integer.parseInt(matcher.group(3));
      startTime = new Calendar.Builder().setDate(year, month - 1, date).build()
          .getTimeInMillis();
    } else if (fileName.endsWith(".log")) {
      Calendar cal = Calendar.getInstance();
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
      startTime = cal.getTimeInMillis();
    }
    return startTime;
  }

  // LLAP Deamon log: llap-deamon-<user>-<nodehost>.log_2017-11-25-00_1.done
  private static final Pattern llapDaemonLogPattern =
      Pattern.compile("log_(\\d+)-(\\d+)-(\\d+)-(\\d+)_\\d+");
  private long getLlapLogsStartTime(String fileName) {
    long startTime = 0;
    Matcher matcher = llapDaemonLogPattern.matcher(fileName);
    if (matcher.find()) {
      int year = Integer.parseInt(matcher.group(1));
      int month = Integer.parseInt(matcher.group(2));
      int date = Integer.parseInt(matcher.group(3));
      int hour = Integer.parseInt(matcher.group(4));
      startTime = new Calendar.Builder().setDate(year, month - 1, date)
          .setTimeOfDay(hour, 0, 0).build().getTimeInMillis();
    } else if (fileName.endsWith(".log")) {
      Calendar cal = Calendar.getInstance();
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
      startTime = cal.getTimeInMillis();
    }
    return startTime;
  }
}
