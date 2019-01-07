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
package com.hortonworks.hivestudio.eventProcessor.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Provider;

import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.dto.StatsProcessingData;
import com.hortonworks.hivestudio.query.entities.repositories.DagDetailsRepository;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;
import com.hortonworks.hivestudio.query.entities.repositories.QueryDetailsRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@DASTransaction
public class ReportProcessingService {
  public final Provider<QueryDetailsRepository> queryDetailsRepositoryProvider;
  public final Provider<DagDetailsRepository> dagDetailsRepositoryProvider;
  public final Provider<HiveQueryRepository> hiveQueryRepositoryProvider;

  @Inject
  public ReportProcessingService(Provider<QueryDetailsRepository> queryDetailsRepositoryProvider,
      Provider<DagDetailsRepository> dagDetailsRepositoryProvider,
      Provider<HiveQueryRepository> hiveQueryRepositoryProvider) {
    this.queryDetailsRepositoryProvider = queryDetailsRepositoryProvider;
    this.dagDetailsRepositoryProvider = dagDetailsRepositoryProvider;
    this.hiveQueryRepositoryProvider = hiveQueryRepositoryProvider;
  }


  public List<StatsProcessingData> getNextQueriesToProcess() {
    QueryDetailsRepository queryDetailsRepository = queryDetailsRepositoryProvider.get();
    List<QueryDetails> queries = queryDetailsRepository.findQueryDetailsForNextSetOfProcessing();
    return getQueryParameters(queries);
  }

  public List<StatsProcessingData> getNextQueriesToProcessByHiveQueryIds(List<Long> ids) {
    if (ids.isEmpty()) {
      log.info("Empty query ids found. Returning empty hive queries list.");
      return new ArrayList<>();
    }
    QueryDetailsRepository queryDetailsRepository = queryDetailsRepositoryProvider.get();
    List<QueryDetails> queries = queryDetailsRepository.findHiveQueryDetailsForNextSetOfProcessingByIds(ids);
    return getQueryParameters(queries);
  }

  public void updateQueriesAsProcessed(List<Long> ids) {
    if (ids.isEmpty()) {
      log.info("Empty query ids found. Skipping update for processed flag.");
      return;
    }
    HiveQueryRepository hiveQueryRepository = hiveQueryRepositoryProvider.get();
    hiveQueryRepository.updateQueriesAsProcessed(ids);
  }

  private List<StatsProcessingData> getQueryParameters(List<QueryDetails> queries) {
    return queries.stream().map(x -> {
      Optional<HiveQuery> hiveQueryOptional = hiveQueryRepositoryProvider.get().findOne(x.getHiveQueryId());
      HiveQuery hiveQuery = hiveQueryOptional.orElseThrow(() -> new ServiceFormattedException(
          "Could not find Hive query associated with the QueryDetails."));
      Collection<DagDetails> dagDetailsList = dagDetailsRepositoryProvider.get()
          .findByHiveQueryId(hiveQuery.getQueryId());
      DagDetails dagDetails = dagDetailsList.isEmpty()
          ? new DagDetails() : dagDetailsList.iterator().next();
      return new StatsProcessingData(x.getHiveQueryId(),
          hiveQuery.getQuery(),
          x.getExplainPlan(),
          hiveQuery.getCreatedAt().toLocalDate(),
          hiveQuery.getTablesWritten(),
          dagDetails.getCounters()
      );
    }).collect(Collectors.toList());
  }
}
