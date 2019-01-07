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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.hadoop.fs.Path;
import org.apache.tez.common.ATSConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.eventProcessor.processors.ProcessingStatus;
import com.hortonworks.hivestudio.eventProcessor.processors.util.ProcessorHelper;
import com.hortonworks.hivestudio.eventdefs.TezHSEvent;
import com.hortonworks.hivestudio.query.entities.repositories.DagDetailsRepository;
import com.hortonworks.hivestudio.query.entities.repositories.DagInfoRepository;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;

public class DagFinishedProcessorTest {

  @Mock Provider<DagInfoRepository> dagInfoRepositoryProvider;
  @Mock Provider<HiveQueryRepository> hiveQueryRepositoryProvider;
  @Mock Provider<DagDetailsRepository> dagDetailsRepositoryProvider;
  @Mock DagInfoRepository dagInfoRepository;
  @Mock HiveQueryRepository hiveQueryRepository;
  @Mock DagDetailsRepository dagDetailsRepository;

  ObjectMapper objectMapper = new ObjectMapper();
  ProcessorHelper helper = new ProcessorHelper(objectMapper);
  DagFinishedProcessor dagFinishedProcessor = null;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(dagInfoRepositoryProvider.get()).thenReturn(dagInfoRepository);
    when(dagDetailsRepositoryProvider.get()).thenReturn(dagDetailsRepository);
    when(hiveQueryRepositoryProvider.get()).thenReturn(hiveQueryRepository);

    dagFinishedProcessor = new DagFinishedProcessor(helper,
        dagInfoRepositoryProvider,
        dagDetailsRepositoryProvider,
        hiveQueryRepositoryProvider,
        objectMapper
    );
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void processValidEvent_DagInfoNotFound(){
    String dagId = "Some-Dag-Id";
    Path filePath = new Path("/path");
    TezHSEvent tezHSEvent = new TezHSEvent();
    tezHSEvent.setDagId(dagId);
    when(dagInfoRepository.findByDagId(dagId)).thenReturn(Optional.empty());
    ProcessingStatus processingStatus = dagFinishedProcessor.processValidEvent(tezHSEvent, filePath);
    String errorMsg = "Dag Info not found for dagId: " + dagId;
    Assert.assertEquals("Status was not error even when DagInfo not found.", ProcessingStatus.Status.ERROR, processingStatus.getStatus());
    Assert.assertTrue("Error was not set event when DagInfo not found.", processingStatus.getError().isPresent());
    Assert.assertTrue("Error instance was not of type RuntimeException.", processingStatus.getError().get() instanceof RuntimeException);
    Assert.assertEquals("Error message incorrect.", errorMsg, processingStatus.getError().get().getMessage() );
  }

  @Test
  public void processValidEvent() {
    String dagId = "Some-Dag-Id";
    Map<String, String> otherInfo = new HashMap<>();
    otherInfo.put(ATSConstants.STATUS, DagInfo.Status.SUCCEEDED.name());
    String countersJson = "{\"counterGroups\":[{\"counterGroupName\":\"org.apache.tez.common.counters.DAGCounter\",\"counters\":[{\"counterName\":\"NUM_SUCCEEDED_TASKS\",\"counterValue\":2},{\"counterName\":\"TOTAL_LAUNCHED_TASKS\",\"counterValue\":2},{\"counterName\":\"DATA_LOCAL_TASKS\",\"counterValue\":1},{\"counterName\":\"AM_CPU_MILLISECONDS\",\"counterValue\":140},{\"counterName\":\"AM_GC_TIME_MILLIS\",\"counterValue\":58}]},{\"counterGroupName\":\"org.apache.tez.common.counters.FileSystemCounter\",\"counterGroupDisplayName\":\"File System Counters\",\"counters\":[{\"counterName\":\"FILE_BYTES_WRITTEN\",\"counterValue\":59},{\"counterName\":\"HDFS_BYTES_WRITTEN\",\"counterValue\":105},{\"counterName\":\"HDFS_READ_OPS\",\"counterValue\":2},{\"counterName\":\"HDFS_WRITE_OPS\",\"counterValue\":2}]},{\"counterGroupName\":\"org.apache.tez.common.counters.TaskCounter\",\"counters\":[{\"counterName\":\"REDUCE_INPUT_GROUPS\",\"counterValue\":1},{\"counterName\":\"REDUCE_INPUT_RECORDS\",\"counterValue\":1},{\"counterName\":\"SPILLED_RECORDS\",\"counterValue\":2},{\"counterName\":\"NUM_SHUFFLED_INPUTS\",\"counterValue\":1},{\"counterName\":\"MERGED_MAP_OUTPUTS\",\"counterValue\":1},{\"counterName\":\"INPUT_RECORDS_PROCESSED\",\"counterValue\":49},{\"counterName\":\"INPUT_SPLIT_LENGTH_BYTES\",\"counterValue\":650355},{\"counterName\":\"OUTPUT_RECORDS\",\"counterValue\":1},{\"counterName\":\"OUTPUT_BYTES\",\"counterValue\":5},{\"counterName\":\"OUTPUT_BYTES_WITH_OVERHEAD\",\"counterValue\":13},{\"counterName\":\"OUTPUT_BYTES_PHYSICAL\",\"counterValue\":27},{\"counterName\":\"ADDITIONAL_SPILLS_BYTES_READ\",\"counterValue\":27},{\"counterName\":\"SHUFFLE_CHUNK_COUNT\",\"counterValue\":1},{\"counterName\":\"SHUFFLE_BYTES\",\"counterValue\":27},{\"counterName\":\"SHUFFLE_BYTES_DECOMPRESSED\",\"counterValue\":13},{\"counterName\":\"SHUFFLE_BYTES_DISK_DIRECT\",\"counterValue\":27},{\"counterName\":\"SHUFFLE_PHASE_TIME\",\"counterValue\":104},{\"counterName\":\"MERGE_PHASE_TIME\",\"counterValue\":105},{\"counterName\":\"FIRST_EVENT_RECEIVED\",\"counterValue\":45},{\"counterName\":\"LAST_EVENT_RECEIVED\",\"counterValue\":45}]},{\"counterGroupName\":\"HIVE\",\"counters\":[{\"counterName\":\"CREATED_FILES\",\"counterValue\":1},{\"counterName\":\"RECORDS_IN_Map_1\",\"counterValue\":50000},{\"counterName\":\"RECORDS_OUT_0\",\"counterValue\":1},{\"counterName\":\"RECORDS_OUT_INTERMEDIATE_Map_1\",\"counterValue\":1}]},{\"counterGroupName\":\"TaskCounter_Map_1_INPUT_customer_address\",\"counters\":[{\"counterName\":\"INPUT_RECORDS_PROCESSED\",\"counterValue\":49},{\"counterName\":\"INPUT_SPLIT_LENGTH_BYTES\",\"counterValue\":650355}]},{\"counterGroupName\":\"TaskCounter_Map_1_OUTPUT_Reducer_2\",\"counters\":[{\"counterName\":\"OUTPUT_BYTES\",\"counterValue\":5},{\"counterName\":\"OUTPUT_BYTES_PHYSICAL\",\"counterValue\":27},{\"counterName\":\"OUTPUT_BYTES_WITH_OVERHEAD\",\"counterValue\":13},{\"counterName\":\"OUTPUT_RECORDS\",\"counterValue\":1},{\"counterName\":\"SHUFFLE_CHUNK_COUNT\",\"counterValue\":1},{\"counterName\":\"SPILLED_RECORDS\",\"counterValue\":1}]},{\"counterGroupName\":\"TaskCounter_Reducer_2_INPUT_Map_1\",\"counters\":[{\"counterName\":\"ADDITIONAL_SPILLS_BYTES_READ\",\"counterValue\":27},{\"counterName\":\"FIRST_EVENT_RECEIVED\",\"counterValue\":45},{\"counterName\":\"LAST_EVENT_RECEIVED\",\"counterValue\":45},{\"counterName\":\"MERGED_MAP_OUTPUTS\",\"counterValue\":1},{\"counterName\":\"MERGE_PHASE_TIME\",\"counterValue\":105},{\"counterName\":\"NUM_SHUFFLED_INPUTS\",\"counterValue\":1},{\"counterName\":\"REDUCE_INPUT_GROUPS\",\"counterValue\":1},{\"counterName\":\"REDUCE_INPUT_RECORDS\",\"counterValue\":1},{\"counterName\":\"SHUFFLE_BYTES\",\"counterValue\":27},{\"counterName\":\"SHUFFLE_BYTES_DECOMPRESSED\",\"counterValue\":13},{\"counterName\":\"SHUFFLE_BYTES_DISK_DIRECT\",\"counterValue\":27},{\"counterName\":\"SHUFFLE_PHASE_TIME\",\"counterValue\":104},{\"counterName\":\"SPILLED_RECORDS\",\"counterValue\":1}]},{\"counterGroupName\":\"org.apache.hadoop.hive.llap.counters.LlapIOCounters\",\"counters\":[{\"counterName\":\"CONSUMER_TIME_NS\",\"counterValue\":62513},{\"counterName\":\"DECODE_TIME_NS\",\"counterValue\":40956},{\"counterName\":\"HDFS_TIME_NS\",\"counterValue\":12858},{\"counterName\":\"METADATA_CACHE_HIT\",\"counterValue\":2},{\"counterName\":\"NUM_DECODED_BATCHES\",\"counterValue\":1},{\"counterName\":\"NUM_VECTOR_BATCHES\",\"counterValue\":49},{\"counterName\":\"ROWS_EMITTED\",\"counterValue\":50000},{\"counterName\":\"SELECTED_ROWGROUPS\",\"counterValue\":5},{\"counterName\":\"TOTAL_IO_TIME_NS\",\"counterValue\":159765}]}]}";
    otherInfo.put(ATSConstants.COUNTERS, countersJson);
    Path filePath = new Path("/path");
    TezHSEvent tezHSEvent = new TezHSEvent();
    tezHSEvent.setEventType("DAG_FINISHED");
    tezHSEvent.setEventTime(1521105062424L);
    tezHSEvent.setDagId(dagId);
    tezHSEvent.setOtherInfo(otherInfo);
    DagInfo dagInfo = new DagInfo();
    HiveQuery hiveQuery = new HiveQuery();
    hiveQuery.setId(1l);
    dagInfo.setHiveQueryId(1l);
    when(dagInfoRepository.findByDagId(dagId)).thenReturn(Optional.ofNullable(dagInfo));
    DagDetails dagDetails = new DagDetails();
    when(dagDetailsRepository.findByDagId(dagId)).thenReturn(Optional.of(dagDetails));
    ProcessingStatus processingStatus = dagFinishedProcessor.processValidEvent(tezHSEvent, filePath);

    verify(hiveQueryRepository, times(1)).updateStats(1l, 1521105062424l, 0l, 0l, 0l, 105l, 0l);
    verify(dagDetailsRepository, times(1)).save(dagDetails);
    Assert.assertEquals("Status was not finished even when event processed successfully.",
        ProcessingStatus.Status.FINISH, processingStatus.getStatus());
  }
}