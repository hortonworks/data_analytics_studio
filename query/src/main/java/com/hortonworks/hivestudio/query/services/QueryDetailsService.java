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
package com.hortonworks.hivestudio.query.services;

import java.util.Collection;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.query.entities.repositories.DagDetailsRepository;
import com.hortonworks.hivestudio.query.entities.repositories.QueryDetailsRepository;

public class QueryDetailsService {
  private final Provider<QueryDetailsRepository> repositoryProvider;
  private final Provider<DagDetailsRepository> dagRepositoryProvider;

  @Inject
  public QueryDetailsService(Provider<QueryDetailsRepository> repositoryProvider,
      Provider<DagDetailsRepository> dagRepositoryProvider) {
    this.repositoryProvider = repositoryProvider;
    this.dagRepositoryProvider = dagRepositoryProvider;
  }

  public QueryDetails getOneByDagId(String dagId) {
    QueryDetailsRepository repository = repositoryProvider.get();
    Optional<QueryDetails> queryDetailsOptional = repository.findByDagId(dagId);
    QueryDetails queryDetails = queryDetailsOptional.orElseThrow(
        () -> new ItemNotFoundException("Query details with dag id '" + dagId + "' not found"));
    Optional<DagDetails> dagDetailsOptional = dagRepositoryProvider.get().findByDagId(dagId);
    if (dagDetailsOptional.isPresent()) {
      // copy value from dagDetailsOptional to queryDetails.
      DagDetails dagDetails = dagDetailsOptional.get();
      queryDetails.setDagPlan(dagDetails.getDagPlan());
      queryDetails.setDiagnostics(dagDetails.getDiagnostics());
      queryDetails.setVertexNameIdMapping(dagDetails.getVertexNameIdMapping());
      queryDetails.setCounters(dagDetails.getCounters());
      queryDetails.setDagInfoId(dagDetails.getDagInfoId());
    }
    return queryDetails;
  }

  public QueryDetails getOneByHiveQueryId(String hiveQueryId) {
    QueryDetailsRepository repository = repositoryProvider.get();
    Optional<QueryDetails> queryDetailsOptional = repository.findByHiveQueryId(hiveQueryId);
    QueryDetails queryDetails = queryDetailsOptional.orElseThrow(
        () -> new ItemNotFoundException("Query details with hive query id '" + hiveQueryId + "' not found"));
    Collection<DagDetails> dagDetailsList = dagRepositoryProvider.get().findByHiveQueryId(hiveQueryId);
    if (dagDetailsList.size() > 0) {
      // TODO: pick the right dag details, this should be based on the query type.
      DagDetails dagDetails = dagDetailsList.iterator().next();
      queryDetails.setDagPlan(dagDetails.getDagPlan());
      queryDetails.setDiagnostics(dagDetails.getDiagnostics());
      queryDetails.setVertexNameIdMapping(dagDetails.getVertexNameIdMapping());
      queryDetails.setCounters(dagDetails.getCounters());
      queryDetails.setDagInfoId(dagDetails.getDagInfoId());
    }
    return queryDetails;
  }
}
