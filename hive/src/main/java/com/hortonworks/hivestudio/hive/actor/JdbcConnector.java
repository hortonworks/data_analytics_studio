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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.apache.hive.jdbc.HiveConnection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.hive.AuthParams;
import com.hortonworks.hivestudio.hive.ConnectionDelegate;
import com.hortonworks.hivestudio.hive.actor.message.Connect;
import com.hortonworks.hivestudio.hive.actor.message.FetchError;
import com.hortonworks.hivestudio.hive.actor.message.FetchResult;
import com.hortonworks.hivestudio.hive.actor.message.GetColumnMetadataJob;
import com.hortonworks.hivestudio.hive.actor.message.GetDatabaseMetadataJob;
import com.hortonworks.hivestudio.hive.actor.message.HiveJob;
import com.hortonworks.hivestudio.hive.actor.message.HiveMessage;
import com.hortonworks.hivestudio.hive.actor.message.ResultInformation;
import com.hortonworks.hivestudio.hive.actor.message.ResultNotReady;
import com.hortonworks.hivestudio.hive.actor.message.RunStatement;
import com.hortonworks.hivestudio.hive.actor.message.SQLStatementJob;
import com.hortonworks.hivestudio.hive.actor.message.job.AuthenticationFailed;
import com.hortonworks.hivestudio.hive.actor.message.job.CancelJob;
import com.hortonworks.hivestudio.hive.actor.message.job.ExecuteNextStatement;
import com.hortonworks.hivestudio.hive.actor.message.job.ExecutionFailed;
import com.hortonworks.hivestudio.hive.actor.message.job.Failure;
import com.hortonworks.hivestudio.hive.actor.message.job.NoResult;
import com.hortonworks.hivestudio.hive.actor.message.job.ResultSetHolder;
import com.hortonworks.hivestudio.hive.actor.message.job.SaveGuidToDB;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.CleanUp;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.DestroyConnector;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.FreeConnector;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.InactivityCheck;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.KeepAlive;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.TerminateInactivityCheck;
import com.hortonworks.hivestudio.hive.client.DatabaseMetadataWrapper;
import com.hortonworks.hivestudio.hive.exceptions.ServiceException;
import com.hortonworks.hivestudio.hive.internal.Connectable;
import com.hortonworks.hivestudio.hive.internal.ConnectionException;
import com.hortonworks.hivestudio.hive.internal.HiveConnectionWrapper;
import com.hortonworks.hivestudio.hive.internal.parsers.DatabaseMetadataExtractor;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.persistence.repositories.JobRepository;
import com.hortonworks.hivestudio.hive.utils.HiveActorConfiguration;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.duration.Duration;


/**
 * Wraps one Jdbc connection per user, per instance. This is used to delegate execute the statements and
 * creates child actors to delegate the ResultSet extraction, YARN/ATS querying for ExecuteJob info and Log Aggregation
 */
@Slf4j
public class JdbcConnector extends HiveActor {

  public static final String SUFFIX = "validating the login";

  /**
   * Interval for maximum inactivity allowed
   */
  private final static long MAX_INACTIVITY_INTERVAL = 5 * 60 * 1000;

  /**
   * Interval for maximum inactivity allowed before termination
   */
  private static final long MAX_TERMINATION_INACTIVITY_INTERVAL = 10 * 60 * 1000;

  private static final long MILLIS_IN_SECOND = 1000L;

  private final Provider<JobRepository> jobRepositoryProvider;
  private final AuthParams authParams;
  private final HiveActorConfiguration actorConfiguration;

  /**
   * Keeps track of the timestamp when the last activity has happened. This is
   * used to calculate the inactivity period and take lifecycle decisions based
   * on it.
   */
  private long lastActivityTimestamp;

  /**
   * Akka scheduler to tick at an interval to deal with inactivity of this actor
   */
  private Cancellable inactivityScheduler;

  /**
   * Akka scheduler to tick at an interval to deal with the inactivity after which
   * the actor should be killed and connection should be released
   */
  private Cancellable terminateActorScheduler;

  private Connectable connectable = null;
  private final ConnectionDelegate connectionDelegate;
  private final ActorRef parent;
  private ActorRef statementExecutor = null;
  private final HdfsApi hdfsApi;
//  private final AuthParams authParams;

  /**
   * true if the actor is currently executing any job.
   */
  private boolean executing = false;
  private HiveJob.Type executionType = HiveJob.Type.SYNC;

  /**
   * Returns the timeout configurations.
   */
//  private final HiveActorConfiguration actorConfiguration;
  private String username;
  private Optional<Integer> jobId = Optional.absent();
  private Optional<String> logFile = Optional.absent();
  private int statementsCount = 0;

  private ActorRef commandSender = null;

  private ActorRef resultSetIterator = null;
  private boolean isFailure = false;
  private Failure failure = null;
  private boolean isCancelCalled = false;

  /**
   * For every execution, this will hold the statements that are left to execute
   */
  private Queue<String> statementQueue = new ArrayDeque<>();

  public JdbcConnector(Configuration configuration, ActorRef parent, HdfsApi hdfsApi,
      ConnectionDelegate connectionDelegate, Provider<JobRepository> jobRepositoryProvider) {
    this.hdfsApi = hdfsApi;
    this.parent = parent;
    this.connectionDelegate = connectionDelegate;
    this.jobRepositoryProvider = jobRepositoryProvider;
    this.lastActivityTimestamp = System.currentTimeMillis();
    resultSetIterator = null;

    authParams = new AuthParams(configuration);
    actorConfiguration = new HiveActorConfiguration(configuration);
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    if (message instanceof InactivityCheck) {
      checkInactivity();
    } else if (message instanceof TerminateInactivityCheck) {
      checkTerminationInactivity();
    } else if (message instanceof KeepAlive) {
      keepAlive();
    } else if (message instanceof CleanUp) {
      cleanUp();
    } else {
      handleNonLifecycleMessage(hiveMessage);
    }
  }

  private void handleNonLifecycleMessage(HiveMessage hiveMessage) {
    Object message = hiveMessage.getMessage();
    keepAlive();
    if (message instanceof Connect) {
      connect((Connect) message);
    } else if (message instanceof SQLStatementJob) {
      runStatementJob((SQLStatementJob) message);
    } else if (message instanceof GetColumnMetadataJob) {
      runGetMetaData((GetColumnMetadataJob) message);
    } else if (message instanceof GetDatabaseMetadataJob) {
      runGetDatabaseMetaData((GetDatabaseMetadataJob) message);
    } else if (message instanceof ExecuteNextStatement) {
      executeNextStatement();
    } else if (message instanceof ResultInformation) {
      gotResultBack((ResultInformation) message);
    } else if (message instanceof CancelJob) {
      cancelJob((CancelJob) message);
    } else if (message instanceof FetchResult) {
      fetchResult((FetchResult) message);
    } else if (message instanceof FetchError) {
      fetchError((FetchError) message);
    } else if (message instanceof SaveGuidToDB) {
      saveGuid((SaveGuidToDB) message);
    } else {
      unhandled(message);
    }
  }

  private void fetchError(FetchError message) {
    if (isFailure) {
      sender().tell(Optional.of(failure), self());
      return;
    }
    sender().tell(Optional.absent(), self());
  }

  private void fetchResult(FetchResult message) {
    if (isFailure) {
      sender().tell(failure, self());
      return;
    }

    if (executing) {
      sender().tell(new ResultNotReady(jobId.get(), username), self());
      return;
    }
    sender().tell(Optional.fromNullable(resultSetIterator), self());
  }

  private void cancelJob(CancelJob message) {
    if (!executing || connectionDelegate == null) {
      log.error("Cannot cancel job for user as currently the job is not running or started. JobId: {}", message.getJobId());
      return;
    }
    log.info("Cancelling job for user. JobId: {}, user: {}", message.getJobId(), username);
    try {
      isCancelCalled = true;
      connectionDelegate.cancel();
      log.info("Cancelled JobId:" + jobId);
    } catch (SQLException e) {
      log.error("Failed to cancel job. JobId: {}. {}", message.getJobId(), e);
    }
  }

  private void gotResultBack(ResultInformation message) {
    Optional<Failure> failureOptional = message.getFailure();
    if (failureOptional.isPresent()) {
      Failure failure = failureOptional.get();
      processFailure(failure);
      return;
    }
    Optional<DatabaseMetaData> databaseMetaDataOptional = message.getDatabaseMetaData();
    if (databaseMetaDataOptional.isPresent()) {
      DatabaseMetaData databaseMetaData = databaseMetaDataOptional.get();
      processDatabaseMetadata(databaseMetaData);
      return;
    }
    if (statementQueue.size() == 0) {
      // This is the last resultSet
      processResult(message.getResultSet());
    }
    self().tell(new ExecuteNextStatement(), self());
  }

  private void processCancel() {
    executing = false;
    if (isAsync() && jobId.isPresent()) {
      log.error("Job canceled by user for JobId: {}", jobId.get());
      updateJobStatus(jobId.get(), Job.JOB_STATE_CANCELED);
    }
  }

  private void processFailure(Failure failure) {
    executing = false;
    isFailure = true;
    this.failure = failure;
    if (isAsync() && jobId.isPresent()) {
      stopStatementExecutor();
      if (isCancelCalled) {
        processCancel();
        return;
      }
      updateJobStatus(jobId.get(), Job.JOB_STATE_ERROR);
    } else {
      // Send for sync execution
      commandSender.tell(new ExecutionFailed(failure.getMessage(), failure.getError()), self());
      cleanUpWithTermination();
    }
  }

  private void processDatabaseMetadata(DatabaseMetaData databaseMetaData) {
    executing = false;
    isFailure = false;
    // Send for sync execution
    try {
      DatabaseMetadataWrapper databaseMetadataWrapper = new DatabaseMetadataExtractor(databaseMetaData).extract();
      commandSender.tell(databaseMetadataWrapper, self());
    } catch (ServiceException e) {
      commandSender.tell(new ExecutionFailed(e.getMessage(), e), self());
    }
    cleanUpWithTermination();
  }

  private void stopStatementExecutor() {
    if (statementExecutor != null) {
      statementExecutor.tell(PoisonPill.getInstance(), ActorRef.noSender());
      statementExecutor = null;
    }
  }

  private void processResult(Optional<ResultSet> resultSetOptional) {
    executing = false;

    stopStatementExecutor();

    log.info("Finished processing SQL statements for Job id : {}", jobId.or(-1)); // -1 is indicative of a SYNC JOB
    if (isAsync() && jobId.isPresent()) {
      updateJobStatus(jobId.get(), Job.JOB_STATE_FINISHED);
    }

    if (resultSetOptional.isPresent()) {
      ActorRef resultSetActor = getContext().actorOf(Props.create(ResultSetIterator.class, self(),
          resultSetOptional.get(), isAsync()).withDispatcher("akka.actor.result-dispatcher"),
          "ResultSetIterator:" + UUID.randomUUID().toString());
      resultSetIterator = resultSetActor;
      if (!isAsync()) {
        commandSender.tell(new ResultSetHolder(resultSetActor), self());
      }
    } else {
      resultSetIterator = null;
      if (!isAsync()) {
        commandSender.tell(new NoResult(), self());
      }
    }
  }

  private void executeNextStatement() {
    if (statementQueue.isEmpty()) {
      jobExecutionCompleted();
      return;
    }

    int index = statementsCount - statementQueue.size();
    String statement = statementQueue.poll();
    if (statementExecutor == null) {
      statementExecutor = getStatementExecutor();
    }

    if (isAsync()) {
      statementExecutor.tell(new RunStatement(index, statement, jobId.get(), true, logFile.get(), true), self());
    } else {
      statementExecutor.tell(new RunStatement(index, statement), self());
    }
  }

  private void runStatementJob(SQLStatementJob message) {
    executing = true;
    jobId = message.getJobId();
    logFile = message.getLogFile();
    executionType = message.getType();
    commandSender = getSender();

    resetToInitialState();

    if (!checkConnection()) return;

    for (String statement : message.getStatements()) {
      statementQueue.add(statement);
    }
    statementsCount = statementQueue.size();

    if (isAsync() && jobId.isPresent()) {
      updateJobStatus(jobId.get(), Job.JOB_STATE_RUNNING);
      startInactivityScheduler();
    }
    self().tell(new ExecuteNextStatement(), self());
  }

  public boolean checkConnection() {
    if (connectable == null) {
      notifyConnectFailure(new SQLException("Hive connection is not created"));
      return false;
    }

    Optional<HiveConnection> connectionOptional = connectable.getConnection();
    if (!connectionOptional.isPresent()) {
      SQLException sqlException = connectable.isUnauthorized()
          ? new SQLException("Hive Connection not Authorized", "AUTHFAIL")
          : new SQLException("Hive connection is not created");
      notifyConnectFailure(sqlException);
      return false;
    }
    return true;
  }

  private void runGetMetaData(GetColumnMetadataJob message) {
    if (!checkConnection()) return;
    resetToInitialState();
    executing = true;
    executionType = message.getType();
    commandSender = getSender();
    statementExecutor = getStatementExecutor();
    statementExecutor.tell(message, self());
  }

  private void runGetDatabaseMetaData(GetDatabaseMetadataJob message) {
    if (!checkConnection()) return;
    resetToInitialState();
    executing = true;
    executionType = message.getType();
    commandSender = getSender();
    statementExecutor = getStatementExecutor();
    statementExecutor.tell(message, self());
  }

  private ActorRef getStatementExecutor() {
    Props props = Props.create(StatementExecutor.class, hdfsApi,
        connectable.getConnection().get(), connectionDelegate)
      .withDispatcher("akka.actor.result-dispatcher");
    return getContext().actorOf(props, "StatementExecutor:" + UUID.randomUUID().toString());
  }

  private boolean isAsync() {
    return executionType == HiveJob.Type.ASYNC;
  }

  private void notifyConnectFailure(Exception ex) {
    boolean loginError = false;
    executing = false;
    isFailure = true;
    this.failure = new Failure("Cannot connect to hive", ex);
    if (ex instanceof ConnectionException) {
      ConnectionException connectionException = (ConnectionException) ex;
      Throwable cause = connectionException.getCause();
      if (cause instanceof SQLException) {
        SQLException sqlException = (SQLException) cause;
        if (isLoginError(sqlException))
          loginError = true;
      }
    }

    if (isAsync()) {
      updateJobStatus(jobId.get(), Job.JOB_STATE_ERROR);

      if (loginError) {
        return;
      }

    } else {
      if (loginError) {
        sender().tell(new AuthenticationFailed("Hive authentication error", ex), ActorRef.noSender());
      } else {
        sender().tell(new ExecutionFailed("Cannot connect to hive", ex), ActorRef.noSender());
      }

    }
    // Do not clean up in case of failed authorizations
    // The failure is bubbled to the user for requesting credentials
    String sqlState = ex instanceof SQLException ? ((SQLException) ex).getSQLState() : null;
    if (sqlState == null || !sqlState.equals("AUTH_FAIL")) {
      cleanUpWithTermination();
    }
  }

  private boolean isLoginError(SQLException ce) {
    return ce.getCause().getMessage().toLowerCase().endsWith(SUFFIX);
  }

  private void keepAlive() {
    lastActivityTimestamp = System.currentTimeMillis();
  }

  private void jobExecutionCompleted() {
    // Set is executing as false so that the inactivity checks can finish cleanup
    // after timeout
    log.info("Job execution completed for user: {}. Results are ready to be fetched", username);
    this.executing = false;
  }

  protected Optional<String> getUsername() {
    return Optional.fromNullable(username);
  }

  @VisibleForTesting
  protected Connectable getConnectable(Connect message, AuthParams authParams) {
    return new HiveConnectionWrapper(message.getJdbcUrl(), message.getUsername(), message.getPassword(), authParams);
  }

  private void connect(Connect message) {
    username = message.getUsername();
    jobId = message.getJobId();
    executionType = message.getType();
    // check the connectable
    if (connectable == null) {
      connectable = getConnectable(message, authParams);
    }
    // make the connectable to Hive
    try {
      if (!connectable.isOpen()) {
        log.debug("Connecting to hive with : {}", connectable);
        connectable.connect();
      }
    } catch (ConnectionException e) {
      log.error("Failed to create a hive connection. {}", e);
      // set up job failure
      // notify parent about job failure
      notifyConnectFailure(e);
      return;
    }
    startTerminateInactivityScheduler();
  }

  private void updateJobStatus(Integer jobid, final String status) {
    new JobSaver(jobid) {
      @Override
      protected void update(Job job) {
        job.setStatus(status);
        job.setDuration(getUpdatedDuration(job.getDateSubmitted()));
      }
    }.save();
    log.info("Stored job status for Job id: {} as '{}'", jobid, status);
  }

  private void saveGuid(final SaveGuidToDB message) {
    new JobSaver(message.getJobId()) {
      @Override
      protected void update(Job job) {
        job.setGuid(message.getGuid());
      }
    }.save();
    log.info("Stored GUID for Job id: {} as '{}'", message.getJobId(), message.getGuid());
  }

  private Long getUpdatedDuration(Long dateSubmitted) {
    return (System.currentTimeMillis() / MILLIS_IN_SECOND) - (dateSubmitted / MILLIS_IN_SECOND);
  }


  private void checkInactivity() {
    log.debug("Inactivity check, executing status: {}", executing);
    if (executing) {
      keepAlive();
      return;
    }
    long current = System.currentTimeMillis();
    if ((current - lastActivityTimestamp) > actorConfiguration.getInactivityTimeout(MAX_INACTIVITY_INTERVAL)) {
      // Stop all the sub-actors created
      cleanUp();
    }
  }

  private void checkTerminationInactivity() {
    long current = System.currentTimeMillis();
    long millisSinceLastActivity = current - lastActivityTimestamp;
    log.debug("Termination check, executing status: {}, millisSinceLastActivity: {}", executing, millisSinceLastActivity);
    if (executing) {
      keepAlive();
      return;
    }

    if (millisSinceLastActivity > actorConfiguration.getTerminationTimeout(MAX_TERMINATION_INACTIVITY_INTERVAL)) {
      cleanUpWithTermination();
    }
  }

  private void cleanUp() {
    if (jobId.isPresent()) {
      log.debug("{} :: Cleaning up resources for inactivity for jobId: {}", self().path().name(), jobId.get());
    } else {
      log.debug("{} ::Cleaning up resources with inactivity for Sync execution.", self().path().name());
    }
    this.executing = false;
    cleanUpStatementAndResultSet();
    stopInactivityScheduler();
    parent.tell(new FreeConnector(username, jobId.orNull(), isAsync()), self());
  }

  private void cleanUpWithTermination() {
    this.executing = false;
    log.debug("{} :: Cleaning up resources with inactivity for execution.", self().path().name());
    cleanUpStatementAndResultSet();

    stopInactivityScheduler();
    stopTerminateInactivityScheduler();
    parent.tell(new DestroyConnector(username, jobId.orNull(), isAsync()), this.self());
    self().tell(PoisonPill.getInstance(), ActorRef.noSender());
  }


  private void cleanUpStatementAndResultSet() {
    connectionDelegate.closeStatement();
    connectionDelegate.closeResultSet();
  }

  protected void startTerminateInactivityScheduler() {
    stopTerminateInactivityScheduler();
    this.terminateActorScheduler = getContext().system().scheduler().schedule(
        Duration.Zero(), Duration.create(60 * 1000, TimeUnit.MILLISECONDS),
        this.getSelf(), new TerminateInactivityCheck(), getContext().dispatcher(), null);
  }

  protected void stopTerminateInactivityScheduler() {
    if (!(terminateActorScheduler == null || terminateActorScheduler.isCancelled())) {
      terminateActorScheduler.cancel();
    }
  }

  protected void startInactivityScheduler() {
    stopInactivityScheduler();
    inactivityScheduler = getContext().system().scheduler().schedule(
        Duration.Zero(), Duration.create(15 * 1000, TimeUnit.MILLISECONDS),
        this.self(), new InactivityCheck(), getContext().dispatcher(), null);
  }

  protected void stopInactivityScheduler() {
    if (inactivityScheduler != null) {
      inactivityScheduler.cancel();
      inactivityScheduler = null;
    }
  }

  private void resetToInitialState() {
    isFailure = false;
    failure = null;
    resultSetIterator = null;
    isCancelCalled = false;
    statementQueue = new ArrayDeque<>();
  }

  @Override
  public void postStop() throws Exception {
    log.info("stopping JdbcConnector actor : {}", self());
    stopInactivityScheduler();
    stopTerminateInactivityScheduler();
    if (connectable != null) {
      log.info("Closing hive connection.");
      try {
        connectable.disconnect();
      } catch (Exception e) {
        log.error("SEVER exception occurred. Failed to close hive connection.", e);
      }
    } else {
      log.error("Found that connectable was null. This may be a BUG.");
    }
  }

  /**
   * Saves the job to database.
   */
  private abstract class JobSaver {
    private final Integer jobId;

    JobSaver(Integer jobId) {
      this.jobId = jobId;
    }

    public void save() {
      save(jobRepositoryProvider.get());
    }

    private void save(JobRepository repository) {
      java.util.Optional<Job> jobOptional = repository.findOne(jobId);
      jobOptional.ifPresent((x) -> {
        update(x);
        repository.save(x);
      });
      if (!jobOptional.isPresent()) {
        itemNotFound(jobId);
      }
    }

    /**
     * Override to handle Not found exception
     */
    private void itemNotFound(Integer jobId) {
      // Nothing to do
      log.error("Job not found with id : {}", jobId);
    }

    protected abstract void update(Job job);
  }
}