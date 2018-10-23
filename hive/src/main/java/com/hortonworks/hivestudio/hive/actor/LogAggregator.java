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


package com.hortonworks.hivestudio.hive.actor;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import com.google.common.base.Joiner;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiException;
import com.hortonworks.hivestudio.common.hdfs.HdfsUtil;
import com.hortonworks.hivestudio.hive.actor.message.GetMoreLogs;
import com.hortonworks.hivestudio.hive.actor.message.HiveMessage;
import com.hortonworks.hivestudio.hive.actor.message.LogAggregationFinished;
import com.hortonworks.hivestudio.hive.actor.message.StartLogAggregation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import scala.concurrent.duration.Duration;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Reads the logs for a ExecuteJob from the Statement and writes them into hdfs.
 */
@Slf4j
public class LogAggregator extends HiveActor {

  public static final int AGGREGATION_INTERVAL = 5 * 1000;
  private final HdfsApi hdfsApi;
  private HiveStatement statement;
  private final String logFile;

  private Cancellable moreLogsScheduler;
  private ActorRef parent;
  private boolean hasStartedFetching = false;
  private boolean shouldFetchMore = true;
  private String allLogs = "";

  public LogAggregator(HdfsApi hdfsApi, String logFile) {
    this.hdfsApi = hdfsApi;
    this.logFile = logFile;
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    if (message instanceof StartLogAggregation) {
      start((StartLogAggregation) message);
    }

    if (message instanceof GetMoreLogs) {
      try {
        getMoreLogs();
      } catch (SQLException e) {
        log.warn("SQL Error while getting logs. Tried writing to: {}. Exception: {}", logFile, e.getMessage());
      } catch (HdfsApiException e) {
        log.warn("HDFS Error while writing logs to {}. Exception: {}", logFile, e.getMessage());

      }
    }
  }

  private void start(StartLogAggregation message) {
    this.statement = message.getHiveStatement();
    parent = this.getSender();
    hasStartedFetching = false;
    shouldFetchMore = true;
    String logTitle = "Logs for Query '" + message.getStatement() + "'";
    String repeatSeperator = StringUtils.repeat("=", logTitle.length());
    allLogs += String.format("\n\n%s\n%s\n%s\n", repeatSeperator, logTitle, repeatSeperator);

    if (!(moreLogsScheduler == null || moreLogsScheduler.isCancelled())) {
      moreLogsScheduler.cancel();
    }
    this.moreLogsScheduler = getContext().system().scheduler().schedule(
      Duration.Zero(), Duration.create(AGGREGATION_INTERVAL, TimeUnit.MILLISECONDS),
      this.getSelf(), new GetMoreLogs(), getContext().dispatcher(), null);
  }

  private void getMoreLogs() throws SQLException, HdfsApiException {
    List<String> logs = statement.getQueryLog();
    if (logs.size() > 0 && shouldFetchMore) {
      allLogs = allLogs + "\n" + Joiner.on("\n").skipNulls().join(logs);
      HdfsUtil.putStringToFile(hdfsApi, logFile, allLogs);
      if(!statement.hasMoreLogs()) {
        shouldFetchMore = false;
      }
    } else {
      // Cancel the timer only when log fetching has been started
      if(!shouldFetchMore) {
        moreLogsScheduler.cancel();
        parent.tell(new LogAggregationFinished(), ActorRef.noSender());
      }
    }
  }

  @Override
  public void postStop() throws Exception {
    if (moreLogsScheduler != null && !moreLogsScheduler.isCancelled()) {
      moreLogsScheduler.cancel();
    }

  }

}
