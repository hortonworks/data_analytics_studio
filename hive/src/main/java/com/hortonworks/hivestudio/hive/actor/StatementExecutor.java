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
import akka.actor.Props;
import com.google.common.base.Optional;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.hive.ConnectionDelegate;
import com.hortonworks.hivestudio.hive.actor.message.GetColumnMetadataJob;
import com.hortonworks.hivestudio.hive.actor.message.GetDatabaseMetadataJob;
import com.hortonworks.hivestudio.hive.actor.message.HiveMessage;
import com.hortonworks.hivestudio.hive.actor.message.ResultInformation;
import com.hortonworks.hivestudio.hive.actor.message.RunStatement;
import com.hortonworks.hivestudio.hive.actor.message.StartLogAggregation;
import com.hortonworks.hivestudio.hive.actor.message.job.Failure;
import com.hortonworks.hivestudio.hive.actor.message.job.UpdateYarnAtsGuid;
import lombok.extern.slf4j.Slf4j;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Executes a single statement and returns the ResultSet if the statements generates ResultSet.
 * Also, starts logAggregation and YarnAtsGuidFetcher if they are required.
 */
@Slf4j
public class StatementExecutor extends HiveActor {

  private final HdfsApi hdfsApi;
  private final HiveConnection connection;
  private final ConnectionDelegate connectionDelegate;
  private ActorRef logAggregator;
  private ActorRef guidFetcher;


  public StatementExecutor(HdfsApi hdfsApi, HiveConnection connection, ConnectionDelegate connectionDelegate) {
    this.hdfsApi = hdfsApi;
    this.connection = connection;
    this.connectionDelegate = connectionDelegate;
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    if (message instanceof RunStatement) {
      runStatement((RunStatement) message);
    } else if (message instanceof GetColumnMetadataJob) {
      getColumnMetaData((GetColumnMetadataJob) message);
    }else if (message instanceof GetDatabaseMetadataJob) {
      getDatabaseMetaData((GetDatabaseMetadataJob) message);
    }
  }

  private void runStatement(RunStatement message) {
    try {
      HiveStatement statement = connectionDelegate.createStatement(connection);
      if (message.shouldStartLogAggregation()) {
        startLogAggregation(statement, message.getStatement(), message.getLogFile().get());
      }

      if (message.shouldStartGUIDFetch() && message.getJobId().isPresent()) {
        startGUIDFetch(message.getId(), statement, message.getJobId().get());
      }
      log.info("Statement executor is executing statement: {}, Statement id: {}, JobId: {}", message.getStatement(), message.getId(), message.getJobId().or(-1));
      Optional<ResultSet> resultSetOptional = connectionDelegate.execute(message.getStatement());
      log.info("Finished executing statement: {}, Statement id: {}, JobId: {}", message.getStatement(), message.getId(), message.getJobId().or(-1));

      if (resultSetOptional.isPresent()) {
        sender().tell(new ResultInformation(message.getId(), resultSetOptional.get()), self());
      } else {
        sender().tell(new ResultInformation(message.getId()), self());
      }
    } catch (SQLException e) {
      log.error("Failed to execute statement: {}. {}", message.getStatement(), e);
      sender().tell(new ResultInformation(message.getId(), new Failure("Failed to execute statement: " + message.getStatement(), e)), self());
    } finally {
      stopGUIDFetch();
    }
  }

  private void startGUIDFetch(int statementId, HiveStatement statement, Integer jobId) {
    if (guidFetcher == null) {
      guidFetcher = getContext().actorOf(Props.create(YarnAtsGUIDFetcher.class, sender())
        .withDispatcher("akka.actor.misc-dispatcher"), "YarnAtsGUIDFetcher:" + UUID.randomUUID().toString());
    }
    log.info("Fetching guid for Job Id: {}", jobId);
    guidFetcher.tell(new UpdateYarnAtsGuid(statementId, statement, jobId), self());
  }

  private void stopGUIDFetch() {
    if (guidFetcher != null) {
      getContext().stop(guidFetcher);
    }
    guidFetcher = null;
  }

  private void startLogAggregation(HiveStatement statement, String sqlStatement, String logFile) {
    if (logAggregator == null) {
      logAggregator = getContext().actorOf(
        Props.create(LogAggregator.class, hdfsApi, logFile)
          .withDispatcher("akka.actor.misc-dispatcher"), "LogAggregator:" + UUID.randomUUID().toString());
    }
    log.info("Fetching query logs for statement: {}", sqlStatement);
    logAggregator.tell(new StartLogAggregation(sqlStatement, statement), getSelf());
  }

  @Override
  public void postStop() {
    log.info("stopping StatementExecutor : {}", getSelf());
    this.logAggregator = null;
  }


  private void getColumnMetaData(GetColumnMetadataJob message) {
    try {
      ResultSet resultSet = connectionDelegate.getColumnMetadata(connection, message);
      sender().tell(new ResultInformation(-1, resultSet), self());
    } catch (SQLException e) {
      log.error("Failed to get column metadata for databasePattern: {}, tablePattern: {}, ColumnPattern {}. {}",
        message.getSchemaPattern(), message.getTablePattern(), message.getColumnPattern(), e);
      sender().tell(new ResultInformation(-1,
        new Failure("Failed to get column metadata for databasePattern: " + message.getSchemaPattern() +
          ", tablePattern: " + message.getTablePattern() + ", ColumnPattern: " + message.getColumnPattern(), e)), self());
    }
  }

  private void getDatabaseMetaData(GetDatabaseMetadataJob message) {
    try {
      DatabaseMetaData metaData = connectionDelegate.getDatabaseMetadata(connection);
      sender().tell(new ResultInformation(-1, metaData), self());
    } catch (SQLException e) {
      log.error("Failed to get database metadata.", e);
      sender().tell(new ResultInformation(-1,
        new Failure("Failed to get database metadata.", e)), self());
    }
  }
}
