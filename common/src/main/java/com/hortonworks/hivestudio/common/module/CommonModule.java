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
package com.hortonworks.hivestudio.common.module;

import javax.inject.Singleton;

import com.hortonworks.hivestudio.common.resource.AdminOnly;
import com.hortonworks.hivestudio.common.resource.AdminOnlyInterceptor;
import com.hortonworks.hivestudio.common.AppAuthentication;
import io.dropwizard.jackson.Jackson;
import org.jdbi.v3.core.Jdbi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.matcher.Matchers;
import com.hortonworks.hivestudio.common.dao.ColumnDao;
import com.hortonworks.hivestudio.common.dao.DBReplicationDao;
import com.hortonworks.hivestudio.common.dao.DatabaseDao;
import com.hortonworks.hivestudio.common.dao.TableDao;
import com.hortonworks.hivestudio.common.dao.TablePartitionInfoDao;
import com.hortonworks.hivestudio.common.dao.VertexInfoDao;
import com.hortonworks.hivestudio.common.persistence.AppPropertyDao;
import com.hortonworks.hivestudio.common.persistence.mappers.JsonArgumentFactory;
import com.hortonworks.hivestudio.common.persistence.mappers.JsonColumnMapper;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;

/**
 * Common injections
 */
public class CommonModule extends AbstractModule {

  private final Jdbi jdbi;
  private AppAuthentication appAuth;

  public CommonModule(Jdbi jdbi, AppAuthentication appAuth){
    this.jdbi = jdbi;
    this.appAuth = appAuth;
  }

  @Override
  protected void configure() {
    // TODO: Find a better place to add this.
    ObjectMapper mapper = new ObjectMapper();
    jdbi.registerColumnMapper(ArrayNode.class, new JsonColumnMapper<>(mapper, ArrayNode.class));
    jdbi.registerColumnMapper(ObjectNode.class, new JsonColumnMapper<>(mapper, ObjectNode.class));
    jdbi.registerArgument(new JsonArgumentFactory(mapper, ArrayNode.class));
    jdbi.registerArgument(new JsonArgumentFactory(mapper, ObjectNode.class));

    TransactionManager txnManager = new TransactionManager(jdbi);
    bind(TransactionManager.class).toInstance(txnManager);
    bindInterceptor(Matchers.any(), Matchers.annotatedWith(DASTransaction.class), txnManager);
    bindInterceptor(Matchers.annotatedWith(DASTransaction.class), Matchers.any(), txnManager);

    AdminOnlyInterceptor adminOnlyInterceptor = new AdminOnlyInterceptor();
    bindInterceptor(Matchers.any(), Matchers.annotatedWith(AdminOnly.class), adminOnlyInterceptor);
    bindInterceptor(Matchers.annotatedWith(AdminOnly.class), Matchers.any(), adminOnlyInterceptor);
  }

  @Provides
  public AppAuthentication getAppAuthentication() {
    return appAuth;
  }

  @Provides
  public AppPropertyDao getAppPropertyDao(TransactionManager txnManager) {
    return txnManager.createDao(AppPropertyDao.class);
  }

  @Provides
  public DBReplicationDao getDBReplicationDao(TransactionManager txnManager) {
    return txnManager.createDao(DBReplicationDao.class);
  }

  @Provides
  public VertexInfoDao getVertexInfoDao(TransactionManager txnManager) {
    return txnManager.createDao(VertexInfoDao.class);
  }

  @Provides
  public TablePartitionInfoDao getTablePartitionInfoDao(TransactionManager txnManager) {
    return txnManager.createDao(TablePartitionInfoDao.class);
  }

  @Provides
  public DatabaseDao getDatabaseDao(TransactionManager txnManager) {
    return txnManager.createDao(DatabaseDao.class);
  }

  @Provides
  public TableDao getTableDao(TransactionManager txnManager) {
    return txnManager.createDao(TableDao.class);
  }

  @Provides
  public ColumnDao getColumnDao(TransactionManager txnManager) {
    return txnManager.createDao(ColumnDao.class);
  }

  @Provides
  @Singleton
  ObjectMapper objectMapper() {
    return Jackson.newObjectMapper();
  }

  @Provides @Singleton
  JacksonJsonProvider jacksonJsonProvider(ObjectMapper mapper) {
    return new JacksonJsonProvider(mapper);
  }
}
