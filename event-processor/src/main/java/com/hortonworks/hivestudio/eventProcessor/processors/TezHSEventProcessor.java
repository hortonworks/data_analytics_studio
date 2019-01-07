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
package com.hortonworks.hivestudio.eventProcessor.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.KVPair;

import com.hortonworks.hivestudio.eventProcessor.processors.tez.DagFinishedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.DagInitializedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.DagStartedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.DagSubmittedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.TaskAttemptFinishedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.TaskAttemptedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.TaskFinishedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.TaskStartedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.VertexConfigureDoneProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.VertexFinishedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.VertexInitializedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.tez.VertexStartedProcessor;
import com.hortonworks.hivestudio.eventdefs.TezHSEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TezHSEventProcessor extends TezEventProcessor {
  private final Map<String, List<TezEventProcessor>> processors = new HashMap<>();

  @Inject
  public TezHSEventProcessor(DagSubmittedProcessor dagSubmittedProcessor,
                             DagInitializedProcessor dagInitializedProcessor,
                             DagStartedProcessor dagStartedProcessor,
                             DagFinishedProcessor dagFinishedProcessor,
                             VertexInitializedProcessor vertexInitializedProcessor,
                             VertexStartedProcessor vertexStartedProcessor,
                             VertexConfigureDoneProcessor vertexConfigureDoneProcessor,
                             VertexFinishedProcessor vertexFinishedProcessor,
                             TaskAttemptedProcessor taskAttemptedProcessor,
                             TaskStartedProcessor taskStartedProcessor,
                             TaskAttemptFinishedProcessor taskAttemptFinishedProcessor,
                             TaskFinishedProcessor taskFinishedProcessor) {
    TezEventProcessor tezProc[] = {
      dagSubmittedProcessor,
      dagInitializedProcessor,
      dagStartedProcessor,
      dagFinishedProcessor,
      vertexInitializedProcessor,
      vertexStartedProcessor,
      vertexConfigureDoneProcessor,
      vertexFinishedProcessor,
      taskAttemptedProcessor,
      taskStartedProcessor,
      taskAttemptFinishedProcessor,
      taskFinishedProcessor
    };
    for (TezEventProcessor processor : tezProc) {
      for (TezEventType evt : processor.validEvents()) {
        String name = evt.name().toUpperCase();
        List<TezEventProcessor> list = processors.get(name);
        if (list == null) {
          list = new ArrayList<>();
          processors.put(name, list);
        }
        list.add(processor);
      }
    }
  }

  @Override
  public ProcessingStatus process(HistoryEventProto event, Path filePath) {
    if (!processors.containsKey(event.getEventType().toUpperCase())) {
      return new ProcessingStatus(ProcessingStatus.Status.SKIP, Optional.empty());
    }
    return processValidEvent(convert(event), filePath);
  }

  private TezHSEvent convert(HistoryEventProto tezProtobufEvent) {
    TezHSEvent tezHSEvent = new TezHSEvent();
    tezHSEvent.setEventType(tezProtobufEvent.getEventType().toString());
    tezHSEvent.setEventTime(tezProtobufEvent.getEventTime());
    tezHSEvent.setUser(tezProtobufEvent.getUser());
    tezHSEvent.setApplicationId(tezProtobufEvent.getAppId());
    tezHSEvent.setApplicationAttemptId(tezProtobufEvent.getAppAttemptId());
    tezHSEvent.setDagId(tezProtobufEvent.getDagId());
    tezHSEvent.setVertexId(tezProtobufEvent.getVertexId());
    tezHSEvent.setTaskId(tezProtobufEvent.getTaskId());
    tezHSEvent.setTaskAttemptId(tezProtobufEvent.getTaskAttemptId());
    tezHSEvent.setOtherInfo(convertListToMap(tezProtobufEvent.getEventDataList()));
    return tezHSEvent;
  }

  private Map<String, String> convertListToMap(List<KVPair> otherInfoList) {
    Map<String, String> otherInfo = new HashMap<>();
    for (KVPair info : otherInfoList) {
      otherInfo.put(info.getKey(), info.getValue());
    }
    return otherInfo;
  }

  @Override
  protected ProcessingStatus processValidEvent(TezHSEvent event, Path filePath) {
    log.info("processing Event of type {}, ", event.getEventType());
    ProcessingStatus processingStatus = null;
    for (TezEventProcessor processor : processors.get(event.getEventType().toUpperCase())) {
      processingStatus = processor.processValidEvent(event, filePath);
      ProcessingStatus.Status status = processingStatus.getStatus();
      if (status == ProcessingStatus.Status.SUCCESS || status == ProcessingStatus.Status.FINISH) {
        log.debug("Event of type {}, processed successfully", event.getEventType());
        break;
      } else if (status == ProcessingStatus.Status.ERROR) {
        log.error("Failed to process event of type {}", event.getEventType(),
            processingStatus.getError().orElseGet(null));
        break;
      }
    }

    if (processingStatus != null && processingStatus.getStatus() == ProcessingStatus.Status.SKIP) {
      log.info("No valid processor found for processing event of type {}", event.getEventType());
    }
    return processingStatus;
  }

  @Override
  protected TezEventType[] validEvents() {
    return TezEventType.values();
  }
}
