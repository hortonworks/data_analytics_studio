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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.fs.Path;
import org.apache.tez.common.ATSConstants;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.dto.Counter;
import com.hortonworks.hivestudio.eventProcessor.dto.CounterGroup;
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
public class DagFinishedProcessor extends TezEventProcessor {

  private final ProcessorHelper helper;
  private final Provider<DagInfoRepository> dagInfoRepositoryProvider;
  private final Provider<DagDetailsRepository> dagDetailsRepositoryProvider;
  private final Provider<HiveQueryRepository> hiveQueryRepository;
  private final ObjectMapper objectMapper;


  @Inject
  public DagFinishedProcessor(ProcessorHelper helper,
                              Provider<DagInfoRepository> dagInfoRepositoryProvider,
                              Provider<DagDetailsRepository> dagDetailsRepositoryProvider,
                              Provider<HiveQueryRepository> hiveQueryRepository,
                              ObjectMapper objectMapper) {
    this.helper = helper;
    this.dagInfoRepositoryProvider = dagInfoRepositoryProvider;
    this.dagDetailsRepositoryProvider = dagDetailsRepositoryProvider;
    this.hiveQueryRepository = hiveQueryRepository;
    this.objectMapper = objectMapper;
  }

  @Override
  @DASTransaction
  protected ProcessingStatus processValidEvent(TezHSEvent event, Path filePath) {
    DagInfoRepository dagInfoRepository = dagInfoRepositoryProvider.get();
    DagDetailsRepository dagDetailsRepository = dagDetailsRepositoryProvider.get();

    String dagId = event.getDagId();
    Optional<DagInfo> dagInfoOptional = dagInfoRepository.findByDagId(dagId);

    if (!dagInfoOptional.isPresent()) {
      log.error("Dag Information not found for dag id: {}. Cannot process event", dagId);
      return new ProcessingStatus(
        ProcessingStatus.Status.ERROR,
        Optional.of(new RuntimeException("Dag Info not found for dagId: " + dagId))
      );
    }

    DagInfo dagInfo = dagInfoOptional.get();

    Map<String, String> otherInfo = event.getOtherInfo();
    dagInfo.setEndTime(event.getEventTime());
    String otherStatus = otherInfo.get(ATSConstants.STATUS);
    DagInfo.Status status = DagInfo.Status.valueOf(otherStatus.toUpperCase());
    dagInfo.setStatus(status.name());
    dagInfo.setSourceFile(filePath.toString());

    ArrayNode counters = getCounters(otherInfo.get(ATSConstants.COUNTERS));
    updateHiveQueryData(dagInfo, counters);

    Optional<DagDetails> dagDetailsOptional = dagDetailsRepository.findByDagId(dagId);
    if(dagDetailsOptional.isPresent()) {
      DagDetails dagDetails = dagDetailsOptional.get();
      dagDetails.setCounters(counters);
      dagDetails.setDiagnostics(otherInfo.get(ATSConstants.DIAGNOSTICS));
      dagDetailsRepository.save(dagDetails);
    }

    dagInfoRepository.save(dagInfo);
    return new ProcessingStatus(ProcessingStatus.Status.FINISH, Optional.empty());
  }

  private Map<String, CounterGroup> getCounterGroupMap(ArrayNode counters) {
    Map<String, CounterGroup> map = new HashMap<>();
    for(JsonNode node : counters) {
      try {
        CounterGroup counterGroup = objectMapper.treeToValue(node, CounterGroup.class);
        map.put(counterGroup.getName(), counterGroup);
      } catch (JsonProcessingException e) {
        log.error("Failed to process json node for getting counters. {}", e);
      }
    }
    return map;
  }


  private void updateHiveQueryData(DagInfo dagInfo, ArrayNode counters) {
    Long endTime = dagInfo.getEndTime();

    Map<String, CounterGroup> groupMap = getCounterGroupMap(counters);
    Long cpuTimeFromCounters = getCpuTimeFromCounters(groupMap);
    Long physicalMemoryFromCounters = getPhysicalMemoryFromCounters(groupMap);
    Long virtualMemoryFromCounters = getVirtualMemoryFromCounters(groupMap);
    Long dataReadFromCounters = getDataReadFromCounters(groupMap);
    Long dataWrittenFromCounters = getDataWrittenFromCounters(groupMap);

    Long id = dagInfo.getHiveQueryId();
    hiveQueryRepository.get().updateStats(id, endTime, cpuTimeFromCounters, physicalMemoryFromCounters,
        virtualMemoryFromCounters, dataReadFromCounters, dataWrittenFromCounters);
    log.info("updating stats of hive_query with id {}.", id);
  }

  private ArrayNode getCounters(String counters) {
    ObjectNode countersNode = helper.parseData(counters, ObjectNode.class);
    return (ArrayNode) countersNode.get("counterGroups");
  }

  private Long getCpuTimeFromCounters(Map<String, CounterGroup> groupMap) {
    CounterGroup dagCounters = groupMap.get("org.apache.tez.common.counters.DAGCounter");
    CounterGroup taskCounters = groupMap.get("org.apache.tez.common.counters.TaskCounter");
    Long cpuMillisDag = getValueFromCounters(dagCounters, "CPU_MILLISECONDS");
    Long cpuMillistasks = getValueFromCounters(taskCounters, "CPU_MILLISECONDS");
    return cpuMillisDag + cpuMillistasks;
  }

  private Long getValueFromCounters(CounterGroup counterGroup, String name) {
    if (counterGroup != null) {
      for (Counter counter : counterGroup.getCounters()) {
        if (counter.getName().equalsIgnoreCase(name)) {
          return Long.valueOf(counter.getValue());
        }
      }
    }
    return 0L;
  }

  private Long getDataWrittenFromCounters(Map<String, CounterGroup> groupMap) {
    CounterGroup fileSystemCounters = groupMap.get("org.apache.tez.common.counters.FileSystemCounter");
    return getValueFromCounters(fileSystemCounters, "HDFS_BYTES_READ");
  }

  private Long getDataReadFromCounters(Map<String, CounterGroup> groupMap) {
    CounterGroup fileSystemCounters = groupMap.get("org.apache.tez.common.counters.FileSystemCounter");
    return getValueFromCounters(fileSystemCounters, "HDFS_BYTES_WRITTEN");
  }

  private Long getVirtualMemoryFromCounters(Map<String, CounterGroup> groupMap) {
    CounterGroup taskCounters = groupMap.get("org.apache.tez.common.counters.TaskCounter");
    return getValueFromCounters(taskCounters, "VIRTUAL_MEMORY_BYTES");
  }

  private Long getPhysicalMemoryFromCounters(Map<String, CounterGroup> groupMap) {
    CounterGroup taskCounters = groupMap.get("org.apache.tez.common.counters.TaskCounter");
    return getValueFromCounters(taskCounters, "PHYSICAL_MEMORY_BYTES");
  }

  @Override
  protected TezEventType[] validEvents() {
    return new TezEventType[]{TezEventType.DAG_FINISHED};
  }
}
