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
package com.hortonworks.hivestudio.eventProcessor.module;

import java.io.IOException;
import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.client.Client;

import org.jdbi.v3.core.Jdbi;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.config.HiveInteractiveConfiguration;
import com.hortonworks.hivestudio.common.config.HiveStudioDefaults;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiSupplier;
import com.hortonworks.hivestudio.common.hdfs.HdfsContext;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;
import com.hortonworks.hivestudio.common.util.PropertyUtils;
import com.hortonworks.hivestudio.eventProcessor.EventProcessorConfiguration;
import com.hortonworks.hivestudio.eventProcessor.configuration.EventProcessingConfig;
import com.hortonworks.hivestudio.eventProcessor.dao.FileStatusDao;
import com.hortonworks.hivestudio.eventProcessor.dao.SchedulerRunAuditDao;
import com.hortonworks.hivestudio.hive.services.ConnectionFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventProcessorModule extends AbstractModule {

  private final Properties akkaProperties;
  private EventProcessingConfig eventProcessingConfig;

  private final EventProcessorConfiguration configuration;
  private final Environment environment;
  private final String configDir;
  private final Jdbi jdbi;

  public EventProcessorModule(EventProcessorConfiguration configuration, Environment environment,
      String configDir, Jdbi jdbi) {
    this.akkaProperties = configuration.getAkkaFactory().getProperties();
    this.eventProcessingConfig = configuration.getEventProcessingConfig();
    this.configuration = configuration;
    this.environment = environment;
    this.configDir = configDir;
    this.jdbi = jdbi;
  }

  @Override
  protected void configure() {
  }

  @Provides
  public FileStatusDao getFileStatusDao(TransactionManager txnManager) {
    return txnManager.createDao(FileStatusDao.class);
  }

  @Provides
  public EventProcessorConfiguration provideEventProcessorConfiguration() {
    return configuration;
  }

  @Provides
  public SchedulerRunAuditDao getSchedulerRunAuditDao(TransactionManager txnManager) {
    return txnManager.createDao(SchedulerRunAuditDao.class);
  }


  @Provides
  @Singleton
  public Jdbi provideJdbi(){
    return jdbi;
  }

  @Provides
  public EventProcessingConfig provideEventProcessingConfig() {
    return eventProcessingConfig;
  }

  @Provides
  @Singleton
  public ConnectionFactory provideConnectionFactory(HiveConfiguration hiveConfiguration,
      Configuration appConfiguration, HiveInteractiveConfiguration hiveInteractiveConfiguration) {
    return new ConnectionFactory(hiveConfiguration, appConfiguration,
        hiveInteractiveConfiguration, null);
  }

  @Provides
  @Singleton
  public GuiceAkkaExtension getGuiceExtension() {
    return new GuiceAkkaExtension();
  }

  @Provides
  @Singleton
  @Inject
  public GuiceAkkaExtension.AkkaExtensionProvider getAkkaExtensionProvider(GuiceAkkaExtension extension, ActorSystem actorSystem) {
    return extension.get(actorSystem);
  }

  @Provides
  @Singleton
  public ActorSystem provideActorSystem(){
    ClassLoader classLoader = getClass().getClassLoader();
    Config config = ConfigFactory.parseProperties(akkaProperties);
    return ActorSystem.create("EventProcessorSystem", config, classLoader);
  }

  @Singleton
  @Provides
  public Client provideJerseyClient(){
    final Client client = new JerseyClientBuilder(environment)
        .using(configuration.getJerseyClientConfiguration())
        .build(EventProcessorModule.class.getName());

    return client;
  }

  @Provides
  @Singleton
  @Inject
  public Configuration provideConfiguration(PropertyUtils propertyUtils){
    // load default properties
    Properties properties = new Properties(HiveStudioDefaults.getDefaultConfigurations());
    propertyUtils.readPropertyFile(properties, this.configDir, Constants.CONFIG_FILE_NAME);

    log.debug("all configurations for hive studio : {}", properties);
    return new Configuration(properties);
  }

  @Provides
  @Inject
  @Singleton
  public HdfsContext provideHdfsContextForEventProcessor(Configuration configuration) {
    String adminUser = System.getProperty("user.name");
    if (adminUser == null) {
      adminUser = "hive";
    }
    return new HdfsContext(configuration.get("hdfs.admin.user", adminUser));
  }

  @Provides
  @Singleton
  @Inject
  public HiveConfiguration provideHiveConfiguration(PropertyUtils propertyUtils){
    // load default properties
    Properties properties = new Properties(HiveStudioDefaults.getDefaultConfigurations());
    propertyUtils.readPropertyFile(properties, this.configDir, Constants.HIVE_SITE_CONFIG_FILE_NAME);

    log.debug("hive site configurations for hive studio : {}", properties);
    return new HiveConfiguration(properties);
  }

  @Provides
  @Singleton
  @Inject
  public HiveInteractiveConfiguration provideHiveInteractiveConfiguration(PropertyUtils propertyUtils){
    // load default properties
    Properties properties = new Properties(HiveStudioDefaults.getDefaultConfigurations());
    propertyUtils.readPropertyFile(properties, this.configDir, Constants.HIVE_INTERACTIVE_SITE_CONFIG_FILE_NAME);

    log.debug("hive interactive site configurations for hive studio : {}", properties);
    return new HiveInteractiveConfiguration(properties);
  }

  @Provides
  @Inject
  @Singleton
  public HdfsApi provideHdfsApi(HdfsContext hdfsContext, Configuration configuration, HdfsApiSupplier hdfsApiSupplier,
                                org.apache.hadoop.conf.Configuration hadoopConfiguration){
    // TODO : correctly create the hdfs api
    try {
      return new HdfsApi(hadoopConfiguration/*, hdfsContext.getUsername()*/);
    } catch (IOException | InterruptedException e) {
      log.error("Exception occurred while creating HDFSApi.", e);
      throw new ServiceFormattedException("Cannot create hdfs api.");
    }
//    Optional<HdfsApi> hdfsApi = hdfsApiSupplier.get(hdfsContext, configuration);
//    if(hdfsApi.isPresent()) return hdfsApi.get();
//    else throw new ServiceFormattedException("Cannot create hdfs api.");
  }
}
