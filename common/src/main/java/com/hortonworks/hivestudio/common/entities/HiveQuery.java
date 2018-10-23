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
package com.hortonworks.hivestudio.common.entities;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.hortonworks.hivestudio.common.orm.EntityTable;
import com.hortonworks.hivestudio.common.orm.annotation.ColumnInfo;
import com.hortonworks.hivestudio.common.orm.annotation.EntityFieldProcessor;
import com.hortonworks.hivestudio.common.orm.annotation.SearchQuery;
import com.hortonworks.hivestudio.common.repository.Identifiable;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Entity for Hive Query
 */

@Data
@NoArgsConstructor
@SearchQuery(prefix = "hq", table="hive_query")
public class HiveQuery implements Identifiable<Long> {

  public static final EntityTable TABLE_INFORMATION = EntityFieldProcessor.process(HiveQuery.class);

  public HiveQuery(Long id,
                   String queryId,
                   String query,
                   String highlightedQuery,
                   Long startTime,
                   Long endTime,
                   Long elapsedTime,
                   String status,
                   String queueName,
                   String userId,
                   String requestUser,
                   Long cpuTime,
                   Long physicalMemory,
                   Long virtualMemory,
                   Long dataRead,
                   Long dataWritten,
                   String operationId,
                   String clientIpAddress,
                   String hiveInstanceAddress,
                   String hiveInstanceType,
                   String sessionId,
                   String logId,
                   String threadId,
                   String executionMode,
                   ArrayNode tablesRead,
                   ArrayNode tablesWritten,
                   String domainId,
                   String llapAppId,
                   String usedCBO) {
    this.id = id;
    this.queryId = queryId;
    this.query = query;
    this.highlightedQuery = highlightedQuery;
    this.startTime = startTime;
    this.endTime = endTime;
    this.elapsedTime = elapsedTime;
    this.status = status;
    this.queueName = queueName;
    this.userId = userId;
    this.requestUser = requestUser;
    this.cpuTime = cpuTime;
    this.physicalMemory = physicalMemory;
    this.virtualMemory = virtualMemory;
    this.dataRead = dataRead;
    this.dataWritten = dataWritten;
    this.operationId = operationId;
    this.clientIpAddress = clientIpAddress;
    this.hiveInstanceAddress = hiveInstanceAddress;
    this.hiveInstanceType = hiveInstanceType;
    this.sessionId = sessionId;
    this.logId = logId;
    this.threadId = threadId;
    this.executionMode = executionMode;
    this.tablesRead = tablesRead;
    this.tablesWritten = tablesWritten;
    this.domainId = domainId;
    this.llapAppId = llapAppId;
    this.usedCBO = usedCBO;
  }

  @ColumnInfo(columnName="id", exclude = true, id=true)
  private Long id;

  @ColumnInfo(columnName="query_id", searchable = true, sortable = true)
  private String queryId;

  @ColumnInfo(columnName="query", tsVectorColumnName = "query_fts", highlightRequired = true,
      highlightProjectionName = "highlighted_query")
  private String query;

  private String highlightedQuery;

  @ColumnInfo(columnName="start_time", searchable = true, sortable = true)
  private Long startTime;

  @ColumnInfo(columnName="end_time", searchable = true, sortable = true)
  private Long endTime;

  @ColumnInfo(columnName="elapsed_time", searchable = true, sortable = true, rangeFacetable = true)
  private Long elapsedTime;

  @ColumnInfo(columnName="status", searchable = true, sortable = true, facetable = true)
  private String status;

  @ColumnInfo(columnName="queue_name", searchable = true, sortable = true, facetable = true)
  private String queueName;

  @ColumnInfo(columnName="user_id", searchable = true, sortable = true, facetable = true)
  private String userId;

  @ColumnInfo(columnName="request_user", searchable = true, sortable = true, facetable = true)
  private String requestUser;

  @ColumnInfo(columnName="cpu_time", searchable = true, sortable = true, rangeFacetable = true)
  private Long cpuTime;

  @ColumnInfo(columnName="physical_memory", searchable = true, sortable = true, rangeFacetable = true)
  private Long physicalMemory;

  @ColumnInfo(columnName="virtual_memory", searchable = true, sortable = true, rangeFacetable = true)
  private Long virtualMemory;

  @ColumnInfo(columnName="data_read", searchable = true, sortable = true, rangeFacetable = true)
  private Long dataRead;

  @ColumnInfo(columnName="data_written", searchable = true, sortable = true, rangeFacetable = true)
  private Long dataWritten;

  @ColumnInfo(columnName="operation_id", searchable = true)
  private String operationId;

  @ColumnInfo(columnName="client_ip_address", searchable = true)
  private String clientIpAddress;

  @ColumnInfo(columnName="hive_instance_address", searchable = true)
  private String hiveInstanceAddress;

  @ColumnInfo(columnName="hive_instance_type", searchable = true)
  private String hiveInstanceType;

  @ColumnInfo(columnName="session_id", searchable = true)
  private String sessionId;

  @ColumnInfo(columnName="log_id", searchable = true)
  private String logId;

  @ColumnInfo(columnName="thread_id", searchable = true)
  private String threadId;

  @ColumnInfo(columnName="execution_mode", searchable = true, facetable = true)
  private String executionMode;

  @ColumnInfo(columnName="tables_read", searchable = true, facetable = true)
  private ArrayNode tablesRead;

  @ColumnInfo(columnName="tables_written", searchable = true, facetable = true)
  private ArrayNode tablesWritten;

  @ColumnInfo(columnName="domain_id", searchable = true)
  private String domainId;

  @ColumnInfo(columnName="llap_app_id", searchable = true)
  private String llapAppId;

  @ColumnInfo(columnName="used_cbo", searchable = true, facetable = true)
  private String usedCBO = "false";

  @JsonIgnore
  @ColumnInfo(columnName="processed", exclude = true)
  private Boolean processed = false;

  @JsonIgnore
  @ColumnInfo(columnName="created_at", exclude = true)
  private LocalDateTime createdAt;

  public enum Status {
    STARTED, RUNNING, SUCCESS, ERROR
  }
}
