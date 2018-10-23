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

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.OtherInfoType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.processors.EventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveEventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveEventType;
import com.hortonworks.hivestudio.eventProcessor.processors.ProcessingStatus;
import com.hortonworks.hivestudio.eventProcessor.processors.util.ProcessorHelper;
import com.hortonworks.hivestudio.eventdefs.HiveHSEvent;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;
import com.hortonworks.hivestudio.query.entities.repositories.QueryDetailsRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QuerySubmittedProcessor extends HiveEventProcessor {

  private final static String CBO_INFO_KEY = "cboInfo";

  private final ProcessorHelper helper;
  private final Provider<HiveQueryRepository> hqRepoProvider;
  private final Provider<QueryDetailsRepository> qdRepoProvider;

  @Inject
  public QuerySubmittedProcessor(ProcessorHelper helper,
      Provider<HiveQueryRepository> hqRepoProvider,
      Provider<QueryDetailsRepository> qdRepoProvider) {
    this.helper = helper;
    this.hqRepoProvider = hqRepoProvider;
    this.qdRepoProvider = qdRepoProvider;
  }

  @DASTransaction
  @Override
  protected ProcessingStatus processValidEvent(HiveHSEvent event) {
    HiveQueryRepository queryRepository = hqRepoProvider.get();
    QueryDetailsRepository queryDetailsRepository = qdRepoProvider.get();
    String hiveQueryId = event.getHiveQueryId();
    boolean isDummyEvent = isDummy(event);
    if (isDummyEvent) {
      log.info("Processing dummy query submitted event generated in the path for other events");
    }

    Optional<HiveQuery> queryOptional = queryRepository.findByHiveQueryId(hiveQueryId);
    Optional<QueryDetails> queryDetails = queryDetailsRepository.findByHiveQueryId(hiveQueryId);

    if (queryOptional.isPresent()) {
      if (isDummyEvent) {
        log.warn("Hive Query with id {} is already present. Dummy event processing not required.", hiveQueryId);
        return new ProcessingStatus(ProcessingStatus.Status.SUCCESS, Optional.empty());
      } else {
        log.info("Hive Query with id {} is already present. Enriching the record.", hiveQueryId);
      }
    }

    QueryDetails details = queryDetails.orElse(new QueryDetails());
    HiveQuery query = queryOptional.orElse(new HiveQuery());

    enrichFromEvent(event, query, details);

    HiveQuery savedHiveQuery = queryRepository.save(query);
    details.setHiveQueryId(savedHiveQuery.getId());
    queryDetailsRepository.save(details);

    return new ProcessingStatus(ProcessingStatus.Status.SUCCESS, Optional.empty());
  }

  private boolean isDummy(HiveHSEvent event) {
    Map<String, String> otherInfo = event.getOtherInfo();
    String isDummy = otherInfo.getOrDefault(EventProcessor.DUMMY_EVENT_KEY, "false");
    return Boolean.valueOf(isDummy);
  }

  @Override
  protected HiveEventType[] validEvents() {
    return new HiveEventType[]{HiveEventType.QUERY_SUBMITTED};
  }

  @VisibleForTesting
  void enrichFromEvent(HiveHSEvent event, final HiveQuery query, final QueryDetails details) {
    query.setQueryId(event.getHiveQueryId());
    query.setStartTime(event.getTimestamp());

    if (query.getStatus() == null) {
      query.setStatus(HiveQuery.Status.STARTED.toString());
    }

    query.setRequestUser(event.getRequestUser());
    query.setUserId(event.getUser());
    query.setExecutionMode(event.getExecutionMode());
    query.setQueueName(event.getQueue());
    query.setOperationId(event.getOperationId());
    query.setTablesWritten(helper.processTablesReadWrite(event.getTablesWritten()));
    query.setTablesRead(helper.processTablesReadWrite(event.getTablesRead()));

    Map<String, String> otherInfo = event.getOtherInfo();

    query.setThreadId(otherInfo.get(OtherInfoType.THREAD_NAME.name()));
    query.setSessionId(otherInfo.get(OtherInfoType.SESSION_ID.name()));
    query.setHiveInstanceAddress(otherInfo.get(OtherInfoType.HIVE_ADDRESS.name()));
    query.setUsedCBO("No");

    QueryData queryData = helper.parseData(
        otherInfo.get(OtherInfoType.QUERY.name()), QueryData.class);
    if (queryData != null) {
      query.setQuery(queryData.getQuery());

      ObjectNode explainPlan = queryData.getExplainPlan();
      details.setExplainPlan(explainPlan);

      if (explainPlan.has(CBO_INFO_KEY)) {
        query.setUsedCBO("Yes");
      }
      details.setExplainPlan(queryData.getExplainPlan());
    }

    details.setConfiguration(helper.parseData(
        otherInfo.get(OtherInfoType.CONF.name()), ObjectNode.class));

    query.setCreatedAt(LocalDateTime.now()); // Needs a revisit for timezone data
  }
}
