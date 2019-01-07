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

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.HashedMap;

import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiSupplier;
import com.hortonworks.hivestudio.hive.ConnectionDelegate;
import com.hortonworks.hivestudio.hive.HiveUtils;
import com.hortonworks.hivestudio.hive.actor.message.Connect;
import com.hortonworks.hivestudio.hive.actor.message.ExecuteJob;
import com.hortonworks.hivestudio.hive.actor.message.FetchError;
import com.hortonworks.hivestudio.hive.actor.message.FetchResult;
import com.hortonworks.hivestudio.hive.actor.message.HiveJob;
import com.hortonworks.hivestudio.hive.actor.message.HiveMessage;
import com.hortonworks.hivestudio.hive.actor.message.JobRejected;
import com.hortonworks.hivestudio.hive.actor.message.RegisterActor;
import com.hortonworks.hivestudio.hive.actor.message.SQLStatementJob;
import com.hortonworks.hivestudio.hive.actor.message.job.CancelJob;
import com.hortonworks.hivestudio.hive.actor.message.job.FetchFailed;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.DestroyConnector;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.FreeConnector;
import com.hortonworks.hivestudio.hive.internal.ContextSupplier;
import com.hortonworks.hivestudio.hive.persistence.repositories.JobRepository;
import com.hortonworks.hivestudio.hive.utils.LoggingOutputStream;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import lombok.extern.slf4j.Slf4j;

/**
 * Router actor to control the operations. This delegates the operations to underlying child actors and
 * store the state for them.
 */
@Slf4j
public class OperationController extends HiveActor {

  private final ActorSystem system;
  private final ActorRef deathWatch;
  private final ContextSupplier<ConnectionDelegate> connectionSupplier;
  private final Provider<JobRepository> jobRepository;
  private final Configuration configuration;
  private final HdfsApiSupplier hdfsApiSupplier;
  private HiveUtils hiveUtils;

  /**
   * Store the connection per user/per job which are currently working.
   */
  private final Map<String, Map<Integer, ActorRef>> asyncBusyConnections;

  /**
   * Store the connection per user which will be used to execute sync jobs
   * like fetching databases, tables etc.
   */
  private final Map<String, Set<ActorRef>> syncBusyConnections;


//  private final HiveContext context;

  @Inject
  public OperationController(ActorSystem actorSystem, Configuration configuration,
      ContextSupplier<ConnectionDelegate> connectionSupplier, Provider<JobRepository> jobRepository,
      HdfsApiSupplier hdfsApiSupplier, HiveUtils hiveUtils) {
    this.system = actorSystem;
    this.deathWatch = actorSystem.actorOf(Props.create(DeathWatch.class));
    this.configuration = configuration;
    this.connectionSupplier = connectionSupplier;
    this.jobRepository = jobRepository;
    this.hdfsApiSupplier = hdfsApiSupplier;
    this.hiveUtils = hiveUtils;
    this.asyncBusyConnections = new HashedMap<>();
    this.syncBusyConnections = new HashMap<>();
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();

    if (message instanceof ExecuteJob) {
      ExecuteJob job = (ExecuteJob) message;
      if (job.getJob().getType() == HiveJob.Type.ASYNC) {
        sendJob(job.getConnect(), (SQLStatementJob) job.getJob());
      } else if (job.getJob().getType() == HiveJob.Type.SYNC) {
        sendSyncJob(job.getConnect(), job.getJob());
      }
    }

    if (message instanceof CancelJob) {
      cancelJob((CancelJob) message);
    }

    if (message instanceof FetchResult) {
      fetchResultActorRef((FetchResult) message);
    }

    if (message instanceof FetchError) {
      fetchError((FetchError) message);
    }

    if (message instanceof FreeConnector) {
      freeConnector((FreeConnector) message);
    }

    if (message instanceof DestroyConnector) {
      destroyConnector((DestroyConnector) message);
    }

//    if (message instanceof SaveDagInformation) {
//      saveDagInformation((SaveDagInformation) message);
//    }
  }

  private void cancelJob(CancelJob message) {
    Integer jobId = message.getJobId();
    String username = message.getHiveContext().getUsername();
    ActorRef actorRef = asyncBusyConnections.get(username).get(jobId);
    if (actorRef != null) {
      actorRef.tell(message, sender());
    } else {
      String msg = String.format("Cannot cancel job. Job with id: %s has either not started or has expired.", message.getJobId());
      log.error(msg);
      sender().tell(new FetchFailed(msg), self());
    }
  }

//  private void saveDagInformation(SaveDagInformation message) {
//    ActorRef jdbcConnection = asyncBusyConnections.get(context.getHiveContext()).get(message.getJobId());
//    if(jdbcConnection != null) {
//      jdbcConnection.tell(message, sender());
//    } else {
//      String msg = String.format("Cannot update Dag Information for job. Job with id: %s has either not started or has expired.", message.getJobId());
//      log.error(msg);
//    }
//  }

  private void fetchError(FetchError message) {
    Integer jobId = message.getJobId();
    String username = message.getHiveContext().getUsername();
    ActorRef actorRef = asyncBusyConnections.get(username).get(jobId);
    if (actorRef != null) {
      actorRef.tell(message, sender());
    } else {
      String msg = String.format("Cannot fetch error for job. Job with id: %s has either not started or has expired.", message.getJobId());
      log.error(msg);
      sender().tell(new FetchFailed(msg), self());
    }
  }

  private void fetchResultActorRef(FetchResult message) {
    try {
      String username = message.getHiveContext().getUsername();
      Integer jobId = message.getJobId();
      ActorRef actorRef = asyncBusyConnections.get(username).get(jobId);
      if (actorRef != null) {
        actorRef.tell(message, sender());
      } else {
        String msg = String.format("Cannot fetch result for job. Job with id: %s  has either not started or has expired.", message.getJobId());
        log.error(msg);
        sender().tell(new FetchFailed(msg), self());
      }
    }catch(Exception e){
      String msg = String.format("Cannot fetch result for job. Job with id: %s  has either not started or has expired.", message.getJobId());
      log.error(msg, e);
      sender().tell(new FetchFailed(msg), self());
    }
  }

  private void sendJob(Connect connect, SQLStatementJob job) {
    String username = job.getHiveContext().getUsername();
    Integer jobId = job.getJobId().get();
    ActorRef subActor = null;
    Optional<HdfsApi> hdfsApiOptional = hdfsApiSupplier.get(hiveUtils.createHdfsContext(job.getHiveContext()));
    if (!hdfsApiOptional.isPresent()) {
      sender().tell(new JobRejected(username, jobId, "Failed to connect to Hive."), self());
      return;
    }
    HdfsApi hdfsApi = hdfsApiOptional.get();

    subActor = system.actorOf(
        Props.create(JdbcConnector.class, configuration, self(),
            hdfsApi, connectionSupplier.get(job.getHiveContext(), configuration),
            jobRepository).withDispatcher("akka.actor.jdbc-connector-dispatcher"),
        UUID.randomUUID().toString() + ":asyncjdbcConnector");
    deathWatch.tell(new RegisterActor(subActor), self());

    if (asyncBusyConnections.containsKey(username)) {
      Map<Integer, ActorRef> actors = asyncBusyConnections.get(username);
      if (!actors.containsKey(jobId)) {
        actors.put(jobId, subActor);
      } else {
        // Reject this as with the same jobId one connection is already in progress.
        sender().tell(new JobRejected(username, jobId, "Existing job in progress with same jobId."), ActorRef.noSender());
      }
    } else {
      Map<Integer, ActorRef> actors = new HashMap<>();
      actors.put(jobId, subActor);
      asyncBusyConnections.put(username, actors);
    }

    // set up the connect with ExecuteJob id for terminations
    subActor.tell(connect, self());
    subActor.tell(job, self());

  }

  private ActorRef getActorRefFromPool(Map<String, Stack<ActorRef>> pool, String username) {
    ActorRef subActor = null;
    if (pool.containsKey(username)) {
      Stack<ActorRef> availableActors = pool.get(username);
      if (availableActors.size() != 0) {
        subActor = availableActors.pop();
      }
    } else {
      pool.put(username, new Stack<ActorRef>());
    }
    return subActor;
  }

  private void sendSyncJob(Connect connect, HiveJob job) {
    String username = job.getHiveContext().getUsername();
    ActorRef subActor = null;
    Optional<HdfsApi> hdfsApiOptional = hdfsApiSupplier.get(hiveUtils.createHdfsContext(job.getHiveContext()));
    if (!hdfsApiOptional.isPresent()) {
      sender().tell(new JobRejected(username, ExecuteJob.SYNC_JOB_MARKER, "Failed to connect to HDFS."), ActorRef.noSender());
      return;
    }
    HdfsApi hdfsApi = hdfsApiOptional.get();

    subActor = system.actorOf(
        Props.create(JdbcConnector.class, configuration, self(),
            hdfsApi, connectionSupplier.get(job.getHiveContext(), configuration),
            jobRepository).withDispatcher("akka.actor.jdbc-connector-dispatcher"),
        UUID.randomUUID().toString() + ":syncjdbcConnector");
    deathWatch.tell(new RegisterActor(subActor), self());

    if (syncBusyConnections.containsKey(username)) {
      Set<ActorRef> actors = syncBusyConnections.get(username);
      actors.add(subActor);
    } else {
      LinkedHashSet<ActorRef> actors = new LinkedHashSet<>();
      actors.add(subActor);
      syncBusyConnections.put(username, actors);
    }

    // Termination requires that the ref is known in case of sync jobs
    subActor.tell(connect, sender());
    subActor.tell(job, sender());
  }


  private void destroyConnector(DestroyConnector message) {
    ActorRef sender = getSender();
    if (message.isForAsync()) {
      removeFromAsyncBusyPool(message.getUsername(), message.getJobId());
    } else {
      removeFromSyncBusyPool(message.getUsername(), sender);
    }
    logMaps();
  }

  private void freeConnector(FreeConnector message) {
    ActorRef sender = getSender();
    if (message.isForAsync()) {
      log.info("About to free connector for job {} and user {}", message.getJobId(), message.getUsername());
      Optional<ActorRef> refOptional = removeFromAsyncBusyPool(message.getUsername(), message.getJobId());
    }else {
      // Was a sync job, remove from sync pool
      log.info("About to free sync connector for user {}", message.getUsername());
      Optional<ActorRef> refOptional = removeFromSyncBusyPool(message.getUsername(), sender);
    }
    logMaps();
  }

  private void logMaps() {
    log.debug("Pool status");
    LoggingOutputStream out = new LoggingOutputStream(log, LoggingOutputStream.LogLevel.DEBUG);
    MapUtils.debugPrint(new PrintStream(out), "Busy Async connections", asyncBusyConnections);
    MapUtils.debugPrint(new PrintStream(out), "Busy Sync connections", syncBusyConnections);
    try {
      out.close();
    } catch (IOException e) {
      log.warn("Cannot close Logging output stream, this may lead to leaks");
    }
  }

  private Optional<ActorRef> removeFromSyncBusyPool(String userName, ActorRef refToFree) {
    if (syncBusyConnections.containsKey(userName)) {
      Set<ActorRef> actorRefs = syncBusyConnections.get(userName);
      actorRefs.remove(refToFree);
    }
    return Optional.of(refToFree);
  }

  private Optional<ActorRef> removeFromAsyncBusyPool(String username, Integer jobId) {
    ActorRef ref = null;
    if (asyncBusyConnections.containsKey(username)) {
      Map<Integer, ActorRef> actors = asyncBusyConnections.get(username);
      if (actors.containsKey(jobId)) {
        ref = actors.get(jobId);
        actors.remove(jobId);
      }
    }
    return Optional.ofNullable(ref);
  }
}


