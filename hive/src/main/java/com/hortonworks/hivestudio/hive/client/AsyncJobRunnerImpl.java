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
package com.hortonworks.hivestudio.hive.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.google.common.base.Optional;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.actor.message.Connect;
import com.hortonworks.hivestudio.hive.actor.message.CursorReset;
import com.hortonworks.hivestudio.hive.actor.message.ExecuteJob;
import com.hortonworks.hivestudio.hive.actor.message.FetchError;
import com.hortonworks.hivestudio.hive.actor.message.FetchResult;
import com.hortonworks.hivestudio.hive.actor.message.ResetCursor;
import com.hortonworks.hivestudio.hive.actor.message.ResultNotReady;
import com.hortonworks.hivestudio.hive.actor.message.SQLStatementJob;
import com.hortonworks.hivestudio.hive.actor.message.job.CancelJob;
import com.hortonworks.hivestudio.hive.actor.message.job.Failure;
import com.hortonworks.hivestudio.hive.actor.message.job.FetchFailed;
import com.hortonworks.hivestudio.hive.internal.ConnectionException;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.utils.ResultFetchFormattedException;
import com.hortonworks.hivestudio.hive.utils.ResultNotReadyFormattedException;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.duration.Duration;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class AsyncJobRunnerImpl implements AsyncJobRunner {

  private final ActorRef controller;
  private final ActorSystem system;
  private final Configuration configuration;
  private final Inbox inbox;

  public AsyncJobRunnerImpl(Configuration configuration, ActorRef controller, ActorSystem system) {
    this.controller = controller;
    this.system = system;
    this.configuration = configuration;
    inbox = Inbox.create(system);
  }


  @Override
  public void submitJob(ConnectionConfig config, SQLStatementJob job, Job jobp) {
    Connect connect = config.createConnectMessage(jobp.getId());
    ExecuteJob executeJob = new ExecuteJob(connect, job);
    controller.tell(executeJob, ActorRef.noSender());
  }

  @Override
  public void cancelJob(Integer jobId, HiveContext hiveContext) {
    controller.tell(new CancelJob(jobId, hiveContext), ActorRef.noSender());
  }

  @Override
  public Optional<NonPersistentCursor> getCursor(Integer jobId, HiveContext hiveContext) throws TimeoutException {
    Inbox inbox = Inbox.create(system);
    inbox.send(controller, new FetchResult(jobId, hiveContext));
    Object receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
    if(receive instanceof ResultNotReady) {
      String errorString = "Result not ready for job: " + jobId + ", username: " + hiveContext.getUsername() + ". Try after sometime.";
      log.info(errorString);
      throw new ResultNotReadyFormattedException(errorString, new Exception(errorString));
    } else if(receive instanceof Failure) {
      Failure failure = (Failure) receive;
      throw new ResultFetchFormattedException(failure.getMessage(), failure.getError());
    } else {
      @SuppressWarnings("unchecked")
      Optional<ActorRef> iterator = (Optional<ActorRef>) receive;
      if(iterator.isPresent()) {
        return Optional.of(new NonPersistentCursor(hiveContext, configuration, system, iterator.get()));
      } else {
        return Optional.absent();
      }
    }
  }

  @Override
  public Optional<NonPersistentCursor> resetAndGetCursor(Integer jobId, HiveContext hiveContext) throws TimeoutException {
    inbox.send(controller, new FetchResult(jobId, hiveContext));
    Object receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
    if(receive instanceof ResultNotReady) {
      String errorString = "Result not ready for job: " + jobId + ", username: " + hiveContext.getUsername() + ". Try after sometime.";
      log.info(errorString);
      throw new ResultNotReadyFormattedException(errorString, new Exception(errorString));
    } else if(receive instanceof  Failure) {
      Failure failure = (Failure) receive;
      throw new ResultFetchFormattedException(failure.getMessage(), failure.getError());
    } else {
      @SuppressWarnings("unchecked")
      Optional<ActorRef> iterator = (Optional<ActorRef>) receive;
      if(iterator.isPresent()) {
        inbox.send(iterator.get(), new ResetCursor());
        Object resetResult = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
        if (resetResult instanceof CursorReset) {
          return Optional.of(new NonPersistentCursor(hiveContext, configuration, system, iterator.get()));
        } else {
          return Optional.absent();
        }
      } else {
        return Optional.absent();
      }
    }
  }

  @Override
  public Optional<Failure> getError(Integer jobId, HiveContext hiveContext) throws TimeoutException {
    Inbox inbox = Inbox.create(system);
    inbox.send(controller, new FetchError(jobId, hiveContext));
    Object receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
    if(receive instanceof FetchFailed){
      FetchFailed fetchFailed = (FetchFailed) receive;
      return Optional.of(new Failure(fetchFailed.getMessage(), getExceptionForRetry()));
    }
    @SuppressWarnings("unchecked")
    Optional<Failure> result = (Optional<Failure>) receive;
    return result;
  }

  private ConnectionException getExceptionForRetry() {
    return new ConnectionException(new SQLException("Cannot connect"),"Connection attempt failed, Please retry");
  }
}
