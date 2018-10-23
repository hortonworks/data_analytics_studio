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
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.MapFieldEntry;

import com.hortonworks.hivestudio.eventProcessor.processors.hive.QueryCompletedProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.hive.QuerySubmittedProcessor;
import com.hortonworks.hivestudio.eventdefs.HiveHSEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveHSEventProcessor extends HiveEventProcessor {

  private final Map<String, List<HiveEventProcessor>> processors = new HashMap<>();

  @Inject
  public HiveHSEventProcessor(QuerySubmittedProcessor submittedProcessor,
      QueryCompletedProcessor completedProcessor) {
    HiveEventProcessor[] hiveProcs = {submittedProcessor, completedProcessor};
    for (HiveEventProcessor processor : hiveProcs) {
      for (HiveEventType evt : processor.validEvents()) {
        String name = evt.name().toUpperCase();
        List<HiveEventProcessor> list = processors.get(name);
        if (list == null) {
          list = new ArrayList<>();
          processors.put(name, list);
        }
        list.add(processor);
      }
    }
  }

  @Override
  public ProcessingStatus process(HiveHookEventProto event, Path filePath) {
    if (!processors.containsKey(event.getEventType().toUpperCase())) {
      return new ProcessingStatus(ProcessingStatus.Status.SKIP, Optional.empty());
    }
    return processValidEvent(convert(event));
  }

  private HiveHSEvent convert(HiveHookEventProto hiveProtobufEvent) {
    HiveHSEvent hiveHSEvent = new HiveHSEvent();
    hiveHSEvent.setEventType(hiveProtobufEvent.getEventType().toString());
    hiveHSEvent.setHiveQueryId(hiveProtobufEvent.getHiveQueryId());
    hiveHSEvent.setTimestamp(hiveProtobufEvent.getTimestamp());
    hiveHSEvent.setExecutionMode(hiveProtobufEvent.getExecutionMode());
    hiveHSEvent.setUser(hiveProtobufEvent.getUser());
    hiveHSEvent.setRequestUser(hiveProtobufEvent.getRequestUser());
    hiveHSEvent.setQueue(hiveProtobufEvent.getQueue());
    hiveHSEvent.setOperationId(hiveProtobufEvent.getOperationId());
    hiveHSEvent.setTablesWritten(hiveProtobufEvent.getTablesWrittenList());
    hiveHSEvent.setTablesRead(hiveProtobufEvent.getTablesReadList());
    hiveHSEvent.setOtherInfo(convertListToMap(hiveProtobufEvent.getOtherInfoList()));
    return hiveHSEvent;
  }

  private Map<String, String> convertListToMap(List<MapFieldEntry> otherInfoList) {
    Map<String, String> otherInfo = new HashMap<>();
    for (MapFieldEntry info : otherInfoList) {
      otherInfo.put(info.getKey(), info.getValue());
    }
    return otherInfo;
  }

  @Override
  protected ProcessingStatus processValidEvent(HiveHSEvent event) {
    log.info("processing Event of type {}, ", event.getEventType());

    ProcessingStatus processingStatus = null;

    for (HiveEventProcessor processor : processors.get(event.getEventType().toUpperCase())) {
      processingStatus = processor.processValidEvent(event);
      ProcessingStatus.Status status = processingStatus.getStatus();
      if (status == ProcessingStatus.Status.SUCCESS) {
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
  protected HiveEventType[] validEvents() {
    return HiveEventType.values();
  }
}
