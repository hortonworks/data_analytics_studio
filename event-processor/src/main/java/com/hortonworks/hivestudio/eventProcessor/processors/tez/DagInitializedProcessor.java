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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.fs.Path;
import org.apache.tez.common.ATSConstants;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.VertexInfo;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.processors.ProcessingStatus;
import com.hortonworks.hivestudio.eventProcessor.processors.TezEventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.TezEventType;
import com.hortonworks.hivestudio.eventProcessor.processors.util.ProcessorHelper;
import com.hortonworks.hivestudio.eventdefs.TezHSEvent;
import com.hortonworks.hivestudio.query.entities.repositories.DagDetailsRepository;
import com.hortonworks.hivestudio.query.entities.repositories.DagInfoRepository;
import com.hortonworks.hivestudio.query.entities.repositories.VertexInfoRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DagInitializedProcessor extends TezEventProcessor {

  private final ProcessorHelper helper;
  private final Provider<DagInfoRepository> dagInfoRepositoryProvider;
  private final Provider<DagDetailsRepository> dagDetailsRepositoryProvider;
  private final Provider<VertexInfoRepository> vertexInfoRepositoryProvider;

  @Inject
  public DagInitializedProcessor(ProcessorHelper helper,
                                 Provider<DagInfoRepository> dagInfoRepositoryProvider,
                                 Provider<DagDetailsRepository> dagDetailsRepositoryProvider,
                                 Provider<VertexInfoRepository> vertexInfoRepositoryProvider) {
    this.helper = helper;
    this.dagInfoRepositoryProvider = dagInfoRepositoryProvider;
    this.dagDetailsRepositoryProvider = dagDetailsRepositoryProvider;
    this.vertexInfoRepositoryProvider = vertexInfoRepositoryProvider;
  }

  @Override
  @DASTransaction
  protected ProcessingStatus processValidEvent(TezHSEvent event, Path filePath) {
    DagInfoRepository dagInfoRepository = dagInfoRepositoryProvider.get();
    DagDetailsRepository dagDetailsRepository = dagDetailsRepositoryProvider.get();

    String dagId = event.getDagId();
    Optional<DagInfo> dagInfoOptional = dagInfoRepository.findByDagId(dagId);

    // TODO: Change all event processors to accept events out of order too, which can happen
    // if new files are created because of different app attempts (recovery).
    if (!dagInfoOptional.isPresent()) {
      log.error("Dag Information not found for dag id: {}. Cannot process event", dagId);
      return new ProcessingStatus(
        ProcessingStatus.Status.ERROR,
        Optional.of(new RuntimeException("Dag Info not found for dagId: " + dagId))
      );
    }

    DagInfo dagInfo = dagInfoOptional.get();

    Map<String, String> otherInfo = event.getOtherInfo();
    dagInfo.setInitTime(event.getEventTime());
    // TODO: We should add the following state
    // dagInfo.setStatus(DagInfo.Status.INITIALIZED.name());

    ObjectNode vertexNameIdMapping = helper.parseData(
        otherInfo.get(ATSConstants.VERTEX_NAME_ID_MAPPING), ObjectNode.class);

    Optional<DagDetails> dagDetailsOptional = dagDetailsRepository.findByDagId(dagId);
    if (dagDetailsOptional.isPresent()) {
      DagDetails dagDetails = dagDetailsOptional.get();
      dagDetails.setVertexNameIdMapping(vertexNameIdMapping);
      dagDetailsRepository.save(dagDetails);
    }

    createVerticesIfNotPresent(vertexNameIdMapping, dagInfo);

    dagInfoRepository.save(dagInfo);
    return new ProcessingStatus(ProcessingStatus.Status.SUCCESS, Optional.empty());
  }

  private void createVerticesIfNotPresent(ObjectNode vertexNameIdMapping, DagInfo dagInfo) {
    VertexInfoRepository repository = vertexInfoRepositoryProvider.get();
    Collection<VertexInfo> allByDagId = repository.findAllByDagId(dagInfo.getDagId());

    Set<String> vertexIds = allByDagId.stream().map(VertexInfo::getVertexId).collect(Collectors.toSet());

    vertexNameIdMapping.fieldNames().forEachRemaining((x) -> {
      String vertexId = vertexNameIdMapping.get(x).asText();
      if(!vertexIds.contains(vertexId)) {
        VertexInfo vertexInfo = new VertexInfo();
        vertexInfo.setVertexId(vertexId);
        vertexInfo.setName(x);
        vertexInfo.setDagId(dagInfo.getId());
        repository.save(vertexInfo);
      }
    });
  }

  @Override
  protected TezEventType[] validEvents() {
    return new TezEventType[]{TezEventType.DAG_INITIALIZED};
  }
}
