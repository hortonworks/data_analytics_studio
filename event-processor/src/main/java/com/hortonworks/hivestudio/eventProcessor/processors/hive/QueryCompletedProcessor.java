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
package com.hortonworks.hivestudio.eventProcessor.processors.hive;

import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveEventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveEventType;
import com.hortonworks.hivestudio.eventProcessor.processors.ProcessingStatus;
import com.hortonworks.hivestudio.eventProcessor.processors.util.ProcessorHelper;
import com.hortonworks.hivestudio.eventdefs.HiveHSEvent;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;
import com.hortonworks.hivestudio.query.entities.repositories.QueryDetailsRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryCompletedProcessor extends HiveEventProcessor {
  private final ProcessorHelper helper;
  private final Provider<HiveQueryRepository> hiveQueryRepositoryProvider;
  private final Provider<QueryDetailsRepository> queryDetailsRepositoryProvider;

  @Inject
  public QueryCompletedProcessor(ProcessorHelper helper,
                                 Provider<HiveQueryRepository> hiveQueryRepositoryProvider,
                                 Provider<QueryDetailsRepository> queryDetailsRepositoryProvider) {
    this.helper = helper;
    this.hiveQueryRepositoryProvider = hiveQueryRepositoryProvider;
    this.queryDetailsRepositoryProvider = queryDetailsRepositoryProvider;
  }


  @Override
  @DASTransaction
  protected ProcessingStatus processValidEvent(HiveHSEvent event) {
    log.info("processing query completed event for {}", event.getHiveQueryId());
    HiveQueryRepository repository = hiveQueryRepositoryProvider.get();
    QueryDetailsRepository queryDetailsRepository = queryDetailsRepositoryProvider.get();
    String hiveQueryId = event.getHiveQueryId();
    Optional<HiveQuery> hiveQueryOptional = repository.findByHiveQueryId(hiveQueryId);
    if (!hiveQueryOptional.isPresent()) {
      log.error("No entry found in database for id {}. Cannot process completed event", hiveQueryId);
      return new ProcessingStatus(ProcessingStatus.Status.ERROR, Optional.of(
        new RuntimeException(
          "No entry found in database for id '" + hiveQueryId + "'. Cannot process completed event"
        )
      ));
    }

    HiveQuery dbQuery = hiveQueryOptional.get();

    dbQuery.setEndTime(event.getTimestamp());

    Map<String, String> otherInfo = event.getOtherInfo();

    Optional<QueryDetails> detailsOptional = queryDetailsRepository.findByHiveQueryId(hiveQueryId);
    if (detailsOptional.isPresent()) {
      QueryDetails details = detailsOptional.get();
      String perf = otherInfo.get(HiveProtoLoggingHook.OtherInfoType.PERF.name());
      details.setPerf(helper.parseData(perf, ObjectNode.class));
      queryDetailsRepository.save(details);
    }

    Boolean status = Boolean.valueOf(
        otherInfo.get(HiveProtoLoggingHook.OtherInfoType.STATUS.name()));
    if(status == true){
      dbQuery.setStatus(HiveQuery.Status.SUCCESS.toString());
    }else{
      dbQuery.setStatus(HiveQuery.Status.ERROR.toString());
    }
    repository.save(dbQuery);
    return new ProcessingStatus(ProcessingStatus.Status.SUCCESS, Optional.empty());
  }

  @Override
  protected HiveEventType[] validEvents() {
    return new HiveEventType[] {HiveEventType.QUERY_COMPLETED};
  }
}
