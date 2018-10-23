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
package com.hortonworks.hivestudio.hive.services;

import com.google.common.collect.Lists;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiException;
import com.hortonworks.hivestudio.common.hdfs.HdfsUtil;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.actor.message.HiveJob;
import com.hortonworks.hivestudio.hive.actor.message.SQLStatementJob;
import com.hortonworks.hivestudio.hive.client.AsyncJobRunner;
import com.hortonworks.hivestudio.hive.client.AsyncJobRunnerImpl;
import com.hortonworks.hivestudio.hive.client.ConnectionConfig;
import com.hortonworks.hivestudio.hive.persistence.entities.IJob;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.resources.jobs.ModifyNotificationDelegate;
import com.hortonworks.hivestudio.hive.resources.jobs.ModifyNotificationInvocationHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class JobControllerImpl implements JobController, ModifyNotificationDelegate {
  private final ConnectionSystem connectionSystem;
  private final Configuration configuration;

  private HiveContext context;
  private HdfsApi hdfsApi;
  private Job jobUnproxied;
  private IJob job;
  private boolean modified;
  private ConnectionFactory connectionFactory;

  /**
   * JobController constructor
   * Warning: Create JobControllers ONLY using JobControllerFactory!
   */
  public JobControllerImpl(HiveContext context, Job job,
                           HdfsApi hdfsApi,
                           ConnectionSystem connectionSystem, Configuration configuration,
                           ConnectionFactory connectionFactory) {
    this.context = context;
    this.connectionFactory = connectionFactory;
    this.hdfsApi = hdfsApi;
    this.connectionSystem = connectionSystem;
    this.configuration = configuration;
    setJobPOJO(job);
  }

  private static final String DEFAULT_DB = "default";

  public String getJobDatabase() {
    if (job.getSelectedDatabase() != null) {
      return job.getSelectedDatabase();
    } else {
      return DEFAULT_DB;
    }
  }


  @Override
  public void submit() throws HiveServiceException {
    String jobDatabase = getJobDatabase();
    String query = job.getQuery();
    AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(configuration, connectionSystem.getOperationController(context), connectionSystem.getActorSystem());
    SQLStatementJob asyncJob = new SQLStatementJob(HiveJob.Type.ASYNC, getStatements(jobDatabase, query), context, job.getId(), job.getLogFile());
    asyncJobRunner.submitJob(getHiveConnectionConfig(), asyncJob, jobUnproxied);

  }

  private String[] getStatements(String jobDatabase, String query) {
    List<String> queries = Lists.asList("use " + jobDatabase, query.split(";"));
    List<String> cleansedQueries = queries.stream()
        .map(s -> s.trim())
        .filter(s -> !StringUtils.isEmpty(s))
        .collect(Collectors.toList());
    return cleansedQueries.toArray(new String[0]);
  }


  @Override
  public void cancel() {
    ConnectionSystem system = connectionSystem;
    AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(configuration, system.getOperationController(context), system.getActorSystem());
    asyncJobRunner.cancelJob(job.getId(), context);
  }

  @Override
  public void update() {
    updateJobDuration();
  }


  @Override
  public IJob getJob() {
    return job;
  }

  /**
   * Use carefully. Returns unproxied bean object
   *
   * @return unproxied bean object
   */
  @Override
  public Job getJobPOJO() {
    return jobUnproxied;
  }

  public void setJobPOJO(Job jobPOJO) {
    IJob jobModifyNotificationProxy = (IJob) Proxy.newProxyInstance(jobPOJO.getClass().getClassLoader(),
        new Class[]{IJob.class},
        new ModifyNotificationInvocationHandler(jobPOJO, this));
    this.job = jobModifyNotificationProxy;

    this.jobUnproxied = jobPOJO;
  }


  @Override
  public void afterCreation() throws HiveServiceException {
    setupStatusDirIfNotPresent();
    setupLogFileIfNotPresent();

    setCreationDate();
  }

  public void setupLogFileIfNotPresent() throws HiveServiceException {
    if (job.getLogFile() == null || job.getLogFile().isEmpty()) {
      setupLogFile();
    }
  }

  public void setupStatusDirIfNotPresent() throws HiveServiceException {
    if (job.getStatusDir() == null || job.getStatusDir().isEmpty()) {
      setupStatusDir();
    }
  }

  private static final long MillisInSecond = 1000L;

  public void updateJobDuration() {
    job.setDuration((System.currentTimeMillis() / MillisInSecond) - (job.getDateSubmitted() / MillisInSecond));
  }

  public void setCreationDate() {
    job.setDateSubmitted(System.currentTimeMillis());
  }

  private void setupLogFile() throws HiveServiceException {
    log.debug("Creating log file for job#" + job.getId());

    String logFile = job.getStatusDir() + "/" + "logs";
    try {
      HdfsUtil.putStringToFile(hdfsApi, logFile, "");
    } catch (HdfsApiException e) {
      throw new HiveServiceException(e);
    }

    job.setLogFile(logFile);
    log.debug("Log file for job#" + job.getId() + ": " + logFile);
  }

  private void setupStatusDir() throws HiveServiceException {
    String newDirPrefix = makeStatusDirectoryPrefix();
    String newDir = null;
    try {
      newDir = HdfsUtil.findUnallocatedFileName(hdfsApi, newDirPrefix, "");
    } catch (HdfsApiException e) {
      throw new ServiceFormattedException(e);
    }

    job.setStatusDir(newDir);
    log.debug("Status dir for job#" + job.getId() + ": " + newDir);
  }

  private String makeStatusDirectoryPrefix() throws HiveServiceException {
    Optional<String> userScriptsPath = configuration.get(Constants.JOBS_DIR);

    if (!userScriptsPath.isPresent()) { // TODO: move check to initialization code
      String msg = Constants.JOBS_DIR + " is not configured!";
      log.error(msg);
      throw new HiveServiceException(msg);
    }

    String normalizedName = String.format("hive-job-%s", job.getId());
    String timestamp = new SimpleDateFormat("yyyy-MM-dd_hh-mm").format(new Date());
    return String.format(userScriptsPath.get() +
        "/%s-%s", normalizedName, timestamp);
  }

  private ConnectionConfig getHiveConnectionConfig() {
    return connectionFactory.create(context);
  }

  @Override
  public boolean onModification(Object object) {
    setModified(true);
    return true;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  public void setModified(boolean modified) {
    this.modified = modified;
  }

  @Override
  public void clearModified() {
    setModified(false);
  }
}
