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
package com.hortonworks.hivestudio.hive;

import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jdbi.v3.core.Jdbi;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.config.HiveInteractiveConfiguration;
import com.hortonworks.hivestudio.common.config.HiveStudioDefaults;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;
import com.hortonworks.hivestudio.common.util.PropertyUtils;
import com.hortonworks.hivestudio.hive.internal.ConnectionSupplier;
import com.hortonworks.hivestudio.hive.internal.ContextSupplier;
import com.hortonworks.hivestudio.hive.persistence.daos.FileDao;
import com.hortonworks.hivestudio.hive.persistence.daos.JobDao;
import com.hortonworks.hivestudio.hive.persistence.daos.SavedQueryDao;
import com.hortonworks.hivestudio.hive.persistence.daos.SettingDao;
import com.hortonworks.hivestudio.hive.persistence.daos.SuggestedSearchDao;
import com.hortonworks.hivestudio.hive.persistence.daos.UdfDao;
import com.hortonworks.hivestudio.hive.services.IJobControllerFactory;
import com.hortonworks.hivestudio.hive.services.JobControllerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveModule extends AbstractModule {

  Jdbi jdbi;
  private final Properties akkaProperties;
  private String configDir;

  public HiveModule(Properties akkaProperties, String configDir, Jdbi jdbi){
    this.akkaProperties = akkaProperties;
    this.configDir = configDir;
    this.jdbi = jdbi;
  }

  @Override
  protected void configure() {
    bind(IJobControllerFactory.class).to(JobControllerFactory.class);
    bind(new TypeLiteral<ContextSupplier<ConnectionDelegate>>(){})
        .to(ConnectionSupplier.class);
  }

  @Provides
  public SavedQueryDao getSavedQueryDao(TransactionManager txnManager) {
    return txnManager.createDao(SavedQueryDao.class);
  }

  @Provides
  public JobDao getJobDao(TransactionManager txnManager) {
    return txnManager.createDao(JobDao.class);
  }

  @Provides
  public UdfDao getUdfDao(TransactionManager txnManager) {
    return txnManager.createDao(UdfDao.class);
  }

  @Provides
  public FileDao getFileDao(TransactionManager txnManager) {
    return txnManager.createDao(FileDao.class);
  }

  @Provides
  public SettingDao getSettingDao(TransactionManager txnManager) {
    return txnManager.createDao(SettingDao.class);
  }

  @Provides
  public SuggestedSearchDao getSuggestedSearchDao(TransactionManager txnManager) {
    return txnManager.createDao(SuggestedSearchDao.class);
  }


  @Provides
  @Singleton
  public GuiceAkkaExtension getGuiceExtension() {
    return new GuiceAkkaExtension();
  }

  @Provides
  @Singleton
  @Inject
  public GuiceAkkaExtension.AkkaExtensionProvider getGuiceExtensionExtension(
      GuiceAkkaExtension extension, ActorSystem actorSystem) {
    return extension.get(actorSystem);
  }

  @Provides
  @Singleton
  @Inject
  public ConnectionSystem provideConnectionSystem(Injector injector, ActorSystem actorSystem,
      Configuration configuration) {
    return new ConnectionSystem(configuration, injector, actorSystem);
  }

  @Provides
  @Singleton
  @Inject
  public HiveConfiguration provideHiveConfiguration(PropertyUtils propertyUtils) {
    // load default properties
    Properties properties = new Properties(HiveStudioDefaults.getDefaultConfigurations());
    propertyUtils.readPropertyFile(properties, this.configDir, Constants.HIVE_SITE_CONFIG_FILE_NAME);

    log.debug("hive site configurations for hive studio : {}", properties);
    return new HiveConfiguration(properties);
  }

  @Provides
  @Singleton
  @Inject
  public HiveInteractiveConfiguration provideHiveInteractiveConfiguration(PropertyUtils propertyUtils) {
    // load default properties
    Properties properties = new Properties(HiveStudioDefaults.getDefaultConfigurations());
    propertyUtils.readPropertyFile(properties, this.configDir, Constants.HIVE_INTERACTIVE_SITE_CONFIG_FILE_NAME);

    log.debug("hive interactive site configurations for hive studio : {}", properties);
    return new HiveInteractiveConfiguration(properties);
  }

  @Provides
  @Singleton
  public ActorSystem provideActorSystem(){
    ClassLoader classLoader = getClass().getClassLoader();
    Config config = ConfigFactory.parseProperties(akkaProperties);
    return ActorSystem.create(ConnectionSystem.ACTOR_SYSTEM_NAME, config, classLoader);
  }
}
