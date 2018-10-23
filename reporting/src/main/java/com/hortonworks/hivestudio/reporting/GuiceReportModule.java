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
package com.hortonworks.hivestudio.reporting;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;
import com.hortonworks.hivestudio.reporting.dao.CSDailyDao;
import com.hortonworks.hivestudio.reporting.dao.CSMonthlyDao;
import com.hortonworks.hivestudio.reporting.dao.CSQuarterlyDao;
import com.hortonworks.hivestudio.reporting.dao.CSWeeklyDao;
import com.hortonworks.hivestudio.reporting.dao.JCSDailyDao;
import com.hortonworks.hivestudio.reporting.dao.JCSMonthlyDao;
import com.hortonworks.hivestudio.reporting.dao.JCSQuarterlyDao;
import com.hortonworks.hivestudio.reporting.dao.JCSWeeklyDao;
import com.hortonworks.hivestudio.reporting.dao.TSDailyDao;
import com.hortonworks.hivestudio.reporting.dao.TSMonthlyDao;
import com.hortonworks.hivestudio.reporting.dao.TSQuarterlyDao;
import com.hortonworks.hivestudio.reporting.dao.TSWeeklyDao;

public class GuiceReportModule extends AbstractModule {
  @Override
  protected void configure() {

  }

  @Provides
  public CSDailyDao getCSDailyDao(TransactionManager txnManager) {
    return txnManager.createDao(CSDailyDao.class);
  }

  @Provides
  public CSMonthlyDao getCSMonthlyDao(TransactionManager txnManager) {
    return txnManager.createDao(CSMonthlyDao.class);
  }

  @Provides
  public CSWeeklyDao getCSWeeklyDao(TransactionManager txnManager) {
    return txnManager.createDao(CSWeeklyDao.class);
  }

  @Provides
  public CSQuarterlyDao getCSQuarterlyDao(TransactionManager txnManager) {
    return txnManager.createDao(CSQuarterlyDao.class);
  }

  @Provides
  public TSDailyDao getTSDailyDao(TransactionManager txnManager) {
    return txnManager.createDao(TSDailyDao.class);
  }

  @Provides
  public TSMonthlyDao getTSMonthlyDao(TransactionManager txnManager) {
    return txnManager.createDao(TSMonthlyDao.class);
  }

  @Provides
  public TSWeeklyDao getTSWeeklyDao(TransactionManager txnManager) {
    return txnManager.createDao(TSWeeklyDao.class);
  }

  @Provides
  public TSQuarterlyDao getTSQuarterlyDao(TransactionManager txnManager) {
    return txnManager.createDao(TSQuarterlyDao.class);
  }

  @Provides
  public JCSDailyDao getJCSDailyDao(TransactionManager txnManager) {
    return txnManager.createDao(JCSDailyDao.class);
  }

  @Provides
  public JCSMonthlyDao getJCSMonthlyDao(TransactionManager txnManager) {
    return txnManager.createDao(JCSMonthlyDao.class);
  }

  @Provides
  public JCSWeeklyDao getJCSWeeklyDao(TransactionManager txnManager) {
    return txnManager.createDao(JCSWeeklyDao.class);
  }

  @Provides
  public JCSQuarterlyDao getJCSQuarterlyDao(TransactionManager txnManager) {
    return txnManager.createDao(JCSQuarterlyDao.class);
  }


}
