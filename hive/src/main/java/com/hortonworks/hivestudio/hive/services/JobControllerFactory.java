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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.hortonworks.hivestudio.common.config.AuthConfig;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiException;
import com.hortonworks.hivestudio.common.hdfs.HdfsContext;
import com.hortonworks.hivestudio.common.hdfs.HdfsUtil;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;

@Singleton
public class JobControllerFactory implements IJobControllerFactory {
  private final ConnectionSystem connectionSystem;
  private final AuthConfig authConfig;
  private final Configuration configuration;
  private final org.apache.hadoop.conf.Configuration hadoopConfiguration;
  private final ConnectionFactory connectionFactory;

  @Inject
  public JobControllerFactory(ConnectionSystem connectionSystem, AuthConfig authConfig,
      Configuration configuration, org.apache.hadoop.conf.Configuration hadoopConfiguration,
      ConnectionFactory connectionFactory) {
    this.connectionSystem = connectionSystem;
    this.authConfig = authConfig;
    this.configuration = configuration;
    this.hadoopConfiguration = hadoopConfiguration;
    this.connectionFactory = connectionFactory;
  }

  @Override
  public JobController createControllerForJob(HiveContext context, Job job) throws HdfsApiException {
    HdfsApi hdfsApi = HdfsUtil.connectToHDFSApi(new HdfsContext(authConfig.getAppUserName()),
        hadoopConfiguration);
    return new JobControllerImpl(context, job, hdfsApi, connectionSystem, configuration,
        connectionFactory);
  }
}
