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

import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.persistence.daos.JobDao;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.persistence.repositories.JobRepository;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiException;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Object that provides CRUD operations for job objects
 */
@Slf4j
public class JobResourceManager{
  private JobRepository jobRepository;
  private IJobControllerFactory jobControllerFactory;

  @Inject
  public JobResourceManager(JobRepository jobRepository, IJobControllerFactory jobControllerFactory){
    this.jobRepository = jobRepository;
    this.jobControllerFactory = jobControllerFactory;
  }

  public Job create(HiveContext hiveContext, Job object) throws HiveServiceException, HdfsApiException {
    jobRepository.save(object);
    JobController jobController = jobControllerFactory.createControllerForJob(hiveContext, object);

    jobController.afterCreation();
    saveIfModified(jobController);

    return object;
  }

  public void saveIfModified(JobController jobController) {
    if (jobController.isModified()) {
      jobRepository.save(jobController.getJobPOJO());
      jobController.clearModified();
    }
  }

  public JobController readController(HiveContext hiveContext, Integer id) {
    Optional<Job> jobOptional = jobRepository.findOne(id);
    return jobControllerFactory.createControllerForJob(hiveContext, jobOptional.orElseThrow(new Supplier<ItemNotFoundException>() {
      @Override
      public ItemNotFoundException get() {
        return new ItemNotFoundException("Job with id : " + id);
      }
    }));
  }
}
