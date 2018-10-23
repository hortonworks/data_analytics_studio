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
package com.hortonworks.hivestudio.query.modules;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Provider;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;
import com.hortonworks.hivestudio.query.entities.daos.DagDetailsDao;
import com.hortonworks.hivestudio.query.entities.daos.DagInfoDao;
import com.hortonworks.hivestudio.query.entities.daos.HiveQueryDao;
import com.hortonworks.hivestudio.query.entities.daos.QueryDetailsDao;
import com.hortonworks.hivestudio.query.parsers.AdvancedSearchParser;
import com.hortonworks.hivestudio.query.parsers.BasicSearchParser;
import com.hortonworks.hivestudio.query.parsers.FacetInputParser;
import com.hortonworks.hivestudio.query.parsers.RangeFacetInputParser;
import com.hortonworks.hivestudio.query.parsers.SearchQueryParser;
import com.hortonworks.hivestudio.query.parsers.SortInputParser;
import com.hortonworks.hivestudio.query.parsers.TimeRangeInputParser;
import com.hortonworks.hivestudio.query.services.SearchService;

/**
 * Guice Module for the query parsers
 */
public class SearchParserModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SearchQueryParser.class)
      .annotatedWith(Names.named("Basic"))
      .toProvider(BasicSearchParserProvider.class)
      .in(Singleton.class);
    bind(SearchQueryParser.class)
      .annotatedWith(Names.named("Advanced"))
      .toProvider(AdvancedSearchParserProvider.class)
      .in(Singleton.class);
  }

  @Provides
  public HiveQueryDao getHiveQueryDao(TransactionManager txnManager) {
    return txnManager.createDao(HiveQueryDao.class);
  }

  @Provides
  public QueryDetailsDao getQueryDetailsDao(TransactionManager txnManager) {
    return txnManager.createDao(QueryDetailsDao.class);
  }

  @Provides
  public DagDetailsDao getDagDetailsDao(TransactionManager txnManager) {
    return txnManager.createDao(DagDetailsDao.class);
  }

  @Provides
  public DagInfoDao getDagInfoDao(TransactionManager txnManager) {
    return txnManager.createDao(DagInfoDao.class);
  }


  @Provides
  @Singleton
  public SortInputParser getSortParser() {
    return new SortInputParser(Stream.concat(
      HiveQuery.TABLE_INFORMATION.getFields().stream(),
      DagInfo.TABLE_INFORMATION.getFields().stream())
      .collect(Collectors.toList())
    );
  }

  @Provides
  @Singleton
  public FacetInputParser getFacetParser() {
    return new FacetInputParser(Stream.concat(
      HiveQuery.TABLE_INFORMATION.getFields().stream(),
      DagInfo.TABLE_INFORMATION.getFields().stream())
      .collect(Collectors.toList())
      , SearchService.getFieldsInformation()
    );
  }

  @Provides
  @Singleton
  public RangeFacetInputParser getRangeFacetParser() {
    return new RangeFacetInputParser(Stream.concat(
      HiveQuery.TABLE_INFORMATION.getFields().stream(),
      DagInfo.TABLE_INFORMATION.getFields().stream())
      .collect(Collectors.toList())
      , SearchService.getFieldsInformation()
    );
  }

  @Provides
  @Singleton
  public TimeRangeInputParser getTimeRangeParser() {
    return new TimeRangeInputParser(HiveQuery.TABLE_INFORMATION);
  }

  public static class BasicSearchParserProvider implements Provider<SearchQueryParser> {
    @Override
    public SearchQueryParser get() {
      return new BasicSearchParser(HiveQuery.TABLE_INFORMATION, DagInfo.TABLE_INFORMATION);
    }
  }

  public static class AdvancedSearchParserProvider implements Provider<SearchQueryParser> {

    @Override
    public SearchQueryParser get() {
      return new AdvancedSearchParser();
    }
  }
}
