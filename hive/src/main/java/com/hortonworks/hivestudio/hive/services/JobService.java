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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.google.common.base.Optional;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiSupplier;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.HiveUtils;
import com.hortonworks.hivestudio.hive.actor.message.job.Failure;
import com.hortonworks.hivestudio.hive.client.AsyncJobRunner;
import com.hortonworks.hivestudio.hive.client.AsyncJobRunnerImpl;
import com.hortonworks.hivestudio.hive.client.NonPersistentCursor;
import com.hortonworks.hivestudio.hive.exceptions.BackgroundJobException;
import com.hortonworks.hivestudio.hive.internal.BackgroundJob;
import com.hortonworks.hivestudio.hive.internal.BackgroundJobController;
import com.hortonworks.hivestudio.hive.persistence.entities.IJob;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.resources.jobs.ResultsPaginationController;
import com.hortonworks.hivestudio.hive.resources.jobs.ResultsResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class JobService {

  private final HdfsApiSupplier hdfsApiSupplier;
  private Configuration configuration;
  private Provider<JobResourceManager> jobResourceManagerProvider = null;
  private ConnectionSystem connectionSystem;
  private ResultsPaginationController resultsPaginationController;
  private BackgroundJobController backgroundJobController;
  private HiveUtils hiveUtils;
  private ConnectionFactory connectionFactory;

  @Inject
  public JobService(Configuration configuration, Provider<JobResourceManager> jobResourceManagerProvider,
                    ConnectionSystem connectionSystem, ResultsPaginationController resultsPaginationController,
                    BackgroundJobController backgroundJobController, HdfsApiSupplier hdfsApiSupplier, HiveUtils hiveUtils,
                    ConnectionFactory connectionFactory) {
    this.configuration = configuration;
    this.jobResourceManagerProvider = jobResourceManagerProvider;
    this.connectionSystem = connectionSystem;
    this.resultsPaginationController = resultsPaginationController;
    this.backgroundJobController = backgroundJobController;
    this.hdfsApiSupplier = hdfsApiSupplier;
    this.hiveUtils = hiveUtils;
    this.connectionFactory = connectionFactory;
  }

  /**
   * Create job
   */
  public Job create(HiveContext hiveContext, Job job) throws HiveServiceException {
    JobResourceManager jobResourceManager = getResourceManager();
    job.setId(null);
    job.setStatus(Job.JOB_STATE_UNKNOWN);
    jobResourceManager.create(hiveContext, job);

    JobController createdJobController = jobResourceManager.readController(hiveContext, job.getId());
    createdJobController.submit();
    jobResourceManager.saveIfModified(createdJobController);
    return createdJobController.getJobPOJO();
  }

  protected synchronized JobResourceManager getResourceManager() {
    return this.jobResourceManagerProvider.get();
  }

  /**
   * Get single item
   */
  @DASTransaction
  public Job getOne(HiveContext hiveContext, Integer jobId) {
    JobController jobController = getResourceManager().readController(hiveContext, jobId);

    IJob job = jobController.getJob();
    if (job.getStatus().equals(Job.JOB_STATE_ERROR) || job.getStatus().equals(Job.JOB_STATE_CANCELED)) {
      final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(configuration, connectionSystem.getOperationController(hiveContext), connectionSystem.getActorSystem());
      Optional<Failure> error = null;
      try {
        error = asyncJobRunner.getError(jobId, hiveContext);
        if (error.isPresent()) {
          Throwable th = error.get().getError();
          if (th instanceof SQLException) {
            SQLException sqlException = (SQLException) th;
            if (sqlException.getSQLState().equals("AUTHFAIL") && connectionFactory.isLdapEnabled())
              throw new HiveServiceException("Hive Authentication failed", sqlException);
          }
          throw new HiveServiceException(th);
        }
      } catch (TimeoutException e) {
        throw new HiveServiceException(e);
      }
    }

    return jobController.getJobPOJO();
  }

  /**
   * Get job results in csv format
   */
  public NonPersistentCursor getResultsCursor(HiveContext hiveContext, Integer jobId,
                                              String fileName) {
    final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(configuration,
        connectionSystem.getOperationController(hiveContext), connectionSystem.getActorSystem());
    Optional<NonPersistentCursor> cursorOptional = null;
    try {
      cursorOptional = asyncJobRunner.resetAndGetCursor(jobId, hiveContext);
    } catch (TimeoutException e) {
      log.error("Error whie getting cursor.", e);
      throw new ServiceFormattedException(e);
    }

    if (!cursorOptional.isPresent()) {
      throw new ServiceFormattedException("Download failed");
    }
    return cursorOptional.get();
  }

  /**
   * save the results to hdfs
   */
  public BackgroundJob getResultsToHDFS(Integer jobId,
                                        String commence,
                                        String targetFile,
                                        String stop,
                                        HiveContext hiveContext) {
    try {
      final JobController jobController = getResourceManager().readController(hiveContext, jobId);

      String backgroundJobId = "csv" + String.valueOf(jobController.getJob().getId());
      if (commence != null && commence.equals("true")) {
        if (targetFile == null)
          throw new ServiceFormattedException("targetFile should not be empty");

        final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(configuration, connectionSystem.getOperationController(hiveContext), connectionSystem.getActorSystem());

        Optional<NonPersistentCursor> cursorOptional = null;

        cursorOptional = asyncJobRunner.resetAndGetCursor(jobId, hiveContext);

        if (!cursorOptional.isPresent()) {
          throw new ServiceFormattedException("Download failed");
        }

        final NonPersistentCursor resultSet = cursorOptional.get();

        // TODO : remove this and create new architecture for background jobs and workflow execution
        backgroundJobController.startJob(String.valueOf(backgroundJobId), new Runnable() {
          @Override
          public void run() {
            try {
              FSDataOutputStream stream = hdfsApiSupplier.get(hiveUtils.createHdfsContext(hiveContext)).get().create(targetFile, true);
              Writer writer = new BufferedWriter(new OutputStreamWriter(stream));
              CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
              try {
                while (resultSet.hasNext() && !Thread.currentThread().isInterrupted()) {
                  csvPrinter.printRecord(resultSet.next().getRow());
                  writer.flush();
                }
              } finally {
                writer.close();
              }
              stream.close();

            } catch (IOException | InterruptedException e) {
              throw new BackgroundJobException("F010 Could not write CSV to HDFS for job#" + jobController.getJob().getId(), e);
            }
          }
        });
      }

      if (stop != null && stop.equals("true")) {
        backgroundJobController.interrupt(backgroundJobId);
      }

      BackgroundJob backgroundJob = new BackgroundJob(backgroundJobController.isInterrupted(backgroundJobId),
          jobController.getJob().getId(), backgroundJobId, "CSV2HDFS", backgroundJobController.state(backgroundJobId).toString());

      return backgroundJob;
    } catch (TimeoutException e) {
      throw new ServiceFormattedException(e);
    }
  }

  public String fetchJobStatus(HiveContext hiveContext, Integer jobId) {
    JobController jobController = getResourceManager().readController(hiveContext, jobId);
    IJob job = jobController.getJob();
    return job.getStatus();
  }

  /**
   * Get next results page
   */
  public ResultsResponse getResults(final Integer jobId,
                                    final String fromBeginning,
                                    Integer count,
                                    String searchId,
                                    final String requestedColumns, HiveContext hiveContext) {
    return resultsPaginationController.getResult(jobId, fromBeginning, count, searchId, requestedColumns, hiveContext, configuration);
  }

  /**
   * Renew expiration time for results
   */
  public void keepAliveResults(Integer jobId) {
    if (!resultsPaginationController.keepAlive(Integer.toString(jobId), ResultsPaginationController.DEFAULT_SEARCH_ID)) {
      throw new ItemNotFoundException("Results already expired");
    }
  }

  public Job createJob(String query, String databaseName, String jobTitle, String referrer, HiveContext hiveContext){
    Job job = new Job();
    job.setTitle(jobTitle);
    job.setQuery(query);
    job.setSelectedDatabase(databaseName);
    job.setReferrer(referrer);

    log.info("creating job : {}", job);
    return this.create(hiveContext, job);
  }

  public Job createJobWithUserSettings(String query, String databaseName, String jobTitle, String referrer,
                                       HiveContext hiveContext){
    Optional<String> settingsString = hiveUtils.getSettingsString(hiveContext.getUsername());
    if (settingsString.isPresent()) {
      query = settingsString.get() + query;
    }
    log.info("Creating job for query : {}", query);
    Job job = new Job();
    job.setTitle(jobTitle);
    job.setQuery(query);
    job.setSelectedDatabase(databaseName);
    job.setReferrer(referrer);

    log.info("creating job : {}", job);
    return this.create(hiveContext, job);
  }

  /**
   * TODO : Get progress info
   */
//  public Response getProgress(Integer jobId) {
//    try {
//      final JobController jobController = getResourceManager().readController(jobId);
//
//      ProgressRetriever.Progress progress = new ProgressRetriever(jobController.getJob(), getSharedObjectsFactory()).
//          getProgress();
//
//      return Response.ok(progress).build();
//    } catch (WebApplicationException ex) {
//      throw ex;
//    } catch (ItemNotFound itemNotFound) {
//      throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
//    } catch (Exception ex) {
//      throw new ServiceFormattedException(ex.getMessage(), ex);
//    }
//  }
}
