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
package com.hortonworks.hivestudio.eventProcessor.processors.tez;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.MapFieldEntry;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.ATSConstants;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.processors.EventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveEventType;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveHSEventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.ProcessingStatus;
import com.hortonworks.hivestudio.eventProcessor.processors.TezEventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.TezEventType;
import com.hortonworks.hivestudio.eventProcessor.processors.util.ProcessorHelper;
import com.hortonworks.hivestudio.eventdefs.TezHSEvent;
import com.hortonworks.hivestudio.query.entities.repositories.DagDetailsRepository;
import com.hortonworks.hivestudio.query.entities.repositories.DagInfoRepository;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DagSubmittedProcessor extends TezEventProcessor {

  private final ProcessorHelper helper;
  private final Provider<HiveQueryRepository> hiveQueryRepositoryProvider;
  private final Provider<DagInfoRepository> dagInfoRepositoryProvider;
  private final Provider<DagDetailsRepository> dagDetailsRepositoryProvider;
  private final HiveHSEventProcessor hiveEventProcessor;

  @Inject
  public DagSubmittedProcessor(ProcessorHelper helper,
                               Provider<HiveQueryRepository> hiveQueryRepositoryProvider,
                               Provider<DagInfoRepository> dagInfoRepositoryProvider,
                               Provider<DagDetailsRepository> dagDetailsRepositoryProvider,
                               HiveHSEventProcessor hiveEventProcessor) {
    this.helper = helper;
    this.hiveQueryRepositoryProvider = hiveQueryRepositoryProvider;
    this.dagInfoRepositoryProvider = dagInfoRepositoryProvider;
    this.dagDetailsRepositoryProvider = dagDetailsRepositoryProvider;
    this.hiveEventProcessor = hiveEventProcessor;
  }

  @Override
  @DASTransaction
  protected ProcessingStatus processValidEvent(TezHSEvent event, Path filePath) {
    HiveQueryRepository hiveQueryRepository = hiveQueryRepositoryProvider.get();
    DagInfoRepository dagInfoRepository = dagInfoRepositoryProvider.get();
    DagDetailsRepository dagDetailsRepository = dagDetailsRepositoryProvider.get();

    Map<String, String> otherInfo = event.getOtherInfo();

    // TODO: We should refine this further by CALLER_CONTEXT_TYPE.
    Long hiveQueryTableId = null;
    String hiveQueryId = otherInfo.get(ATSConstants.CALLER_CONTEXT_ID);
    if (hiveQueryId != null) {
      Optional<HiveQuery> firstHiveQuery = getHiveQueryFromRepository(hiveQueryRepository, hiveQueryId);

      if (!firstHiveQuery.isPresent()) {
        log.warn("Processing {} event for DagId: {}. Hive Query not found with id: {}. " +
            "Processing a dummy 'QUERY_SUBMITTED' event.", event.getEventType(), event.getDagId(),
            hiveQueryId);
        HiveHookEventProto eventProto = createDummyQuerySubmittedEvent(event);
        hiveEventProcessor.process(eventProto, filePath);
        firstHiveQuery = getHiveQueryFromRepository(hiveQueryRepository, hiveQueryId);
      }
      hiveQueryTableId = firstHiveQuery.get().getId();
    } else {
      log.warn("Caller context id is null for dag_id: {}", event.getDagId());
    }

    // Check if the dag information is already present, then update it.
    Optional<DagInfo> firstDagInfo = dagInfoRepository.findByDagId(event.getDagId());

    DagInfo dagInfo;
    if (firstDagInfo.isPresent()) {
      log.warn("Dag information for dag id {} already present. Updating the record.", event.getDagId());
      dagInfo = firstDagInfo.get();
    } else {
      dagInfo = new DagInfo();
      dagInfo.setCreatedAt(LocalDateTime.now()); // Needs a revisit for timezone data.
    }

    dagInfo.setDagId(event.getDagId());
    dagInfo.setApplicationId(event.getApplicationId());
    dagInfo.setDagName(otherInfo.get(ATSConstants.DAG_NAME));
    dagInfo.setStartTime(event.getEventTime());
    dagInfo.setStatus(DagInfo.Status.SUBMITTED.name());
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.fromString(event.getApplicationAttemptId());
    dagInfo.setAmLogUrl(otherInfo.get(ATSConstants.IN_PROGRESS_LOGS_URL + "_" +
        appAttemptId.getAttemptId()));
    dagInfo.setHiveQueryId(hiveQueryTableId);
    dagInfo.setQueueName(otherInfo.get(ATSConstants.DAG_QUEUE_NAME));
    dagInfo.setCallerId(otherInfo.get(ATSConstants.CALLER_CONTEXT_ID));
    dagInfo.setCallerType(otherInfo.get(ATSConstants.CALLER_CONTEXT_TYPE));
    dagInfo.setAmWebserviceVer(otherInfo.get(ATSConstants.DAG_AM_WEB_SERVICE_VERSION));
    dagInfo.setSourceFile(filePath.toString());
    dagInfoRepository.save(dagInfo);

    DagDetails dagDetails = null;
    Optional<DagDetails> dagDetailsOptional = dagDetailsRepository.findByDagId(event.getDagId());
    if (dagDetailsOptional.isPresent()) {
      dagDetails = dagDetailsOptional.get();
    } else {
      dagDetails = new DagDetails();
      dagDetails.setHiveQueryId(hiveQueryTableId);
    }
    dagDetails.setDagInfoId(dagInfo.getId());
    dagDetails.setDagPlan(helper.parseData(otherInfo.get(ATSConstants.DAG_PLAN),
        ObjectNode.class));
    dagDetailsRepository.save(dagDetails);

    return new ProcessingStatus(ProcessingStatus.Status.SUCCESS, Optional.empty());
  }

  private Optional<HiveQuery> getHiveQueryFromRepository(
    HiveQueryRepository hiveQueryRepository, String hiveQueryId) {
    Optional<HiveQuery> byHiveQueryId = hiveQueryRepository.findByHiveQueryId(hiveQueryId);
    return byHiveQueryId;
  }

  private HiveHookEventProto createDummyQuerySubmittedEvent(TezHSEvent event) {
    HiveHookEventProto.Builder builder = HiveHookEventProto.newBuilder();
    builder.setEventType(HiveEventType.QUERY_SUBMITTED.toString());
    Map<String, String> otherInfo = event.getOtherInfo();
    String hiveQueryId = otherInfo.get(ATSConstants.CALLER_CONTEXT_ID);
    builder.setHiveQueryId(hiveQueryId);
    builder.setTimestamp(event.getEventTime());
    builder.setUser(event.getUser());
    builder.setRequestUser(event.getUser());
    builder.addOtherInfo(MapFieldEntry.newBuilder().setKey(EventProcessor.DUMMY_EVENT_KEY)
        .setValue("true").build());
    return builder.build();
  }

  @Override
  protected TezEventType[] validEvents() {
    return new TezEventType[]{TezEventType.DAG_SUBMITTED};
  }
}
