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
package com.hortonworks.hivestudio.common.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;

import lombok.Getter;

@Getter
public class HiveQueryDto {

  private QueryDetails details;
  private DagInfo dagInfo;

  private final Long id;
  private final String queryId;
  private final Long startTime;
  private final String query;
  private final String highlightedQuery;
  private final Long endTime;
  private final Long elapsedTime;
  private final String status;
  private final String queueName;
  private final String userId;
  private final String requestUser;
  private final Long cpuTime;
  private final Long physicalMemory;
  private final Long virtualMemory;
  private final Long dataRead;
  private final Long dataWritten;
  private final String operationId;
  private final String clientIpAddress;
  private final String hiveInstanceAddress;
  private final String hiveInstanceType;
  private final String sessionId;
  private final String logId;
  private final String threadId;
  private final String executionMode;
  private final ArrayNode tablesRead;
  private final ArrayNode tablesWritten;
  private final String domainId;
  private final String llapAppId;
  private final String usedCBO;
  private final Boolean processed;
  private final LocalDateTime createdAt;

  public HiveQueryDto(HiveQuery hiveQuery){
    this.id = hiveQuery.getId();
    this.queryId = hiveQuery.getQueryId();
    this.query = hiveQuery.getQuery();
    this.highlightedQuery = hiveQuery.getHighlightedQuery();
    this.startTime = hiveQuery.getStartTime();
    this.endTime = hiveQuery.getEndTime();
    this.elapsedTime = hiveQuery.getElapsedTime();
    this.status = hiveQuery.getStatus();
    this.queueName = hiveQuery.getQueueName();
    this.userId = hiveQuery.getUserId();
    this.requestUser = hiveQuery.getRequestUser();
    this.cpuTime = hiveQuery.getCpuTime();
    this.physicalMemory = hiveQuery.getPhysicalMemory();
    this.virtualMemory = hiveQuery.getVirtualMemory();
    this.dataRead = hiveQuery.getDataRead();
    this.dataWritten = hiveQuery.getDataWritten();
    this.operationId = hiveQuery.getOperationId();
    this.clientIpAddress = hiveQuery.getClientIpAddress();
    this.hiveInstanceAddress = hiveQuery.getHiveInstanceAddress();
    this.hiveInstanceType = hiveQuery.getHiveInstanceType();
    this.sessionId = hiveQuery.getSessionId();
    this.logId = hiveQuery.getLogId();
    this.threadId = hiveQuery.getThreadId();
    this.executionMode = hiveQuery.getExecutionMode();
    this.tablesRead = hiveQuery.getTablesRead();
    this.tablesWritten = hiveQuery.getTablesWritten();
    this.domainId = hiveQuery.getDomainId();
    this.llapAppId = hiveQuery.getLlapAppId();
    this.usedCBO = hiveQuery.getUsedCBO();
    this.processed = hiveQuery.getProcessed();
    this.createdAt = hiveQuery.getCreatedAt();
  }

  public HiveQueryDto(HiveQuery hiveQuery, QueryDetails details, DagInfo dagInfo){
    this(hiveQuery);
    this.details = details;
    this.dagInfo = dagInfo;
  }

  public void setQueryDetails(QueryDetails details) {
    this.details = details;
  }

  public void setDagInfo(DagInfo dagInfo) {
    this.dagInfo = dagInfo;
  }
}
