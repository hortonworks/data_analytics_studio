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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.hortonworks.hivestudio.common.orm.EntityTable;
import com.hortonworks.hivestudio.common.orm.annotation.ColumnInfo;
import com.hortonworks.hivestudio.common.orm.annotation.EntityFieldProcessor;
import com.hortonworks.hivestudio.common.orm.annotation.SearchQuery;
import com.hortonworks.hivestudio.common.repository.Identifiable;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Entity for representing the dag information
 */
@Data
@ToString(exclude = {"details"}) // excluded big objects
@NoArgsConstructor
@SearchQuery(prefix = "di", table="dag_info")
public class DagInfo implements Identifiable<Long> {
  public static final EntityTable TABLE_INFORMATION = EntityFieldProcessor.process(DagInfo.class);

  public DagInfo(Long id,
                 String dagId,
                 String dagName,
                 String applicationId,
                 Long initTime,
                 Long startTime,
                 Long endTime,
                 String status,
                 String amWebserviceVer,
                 String amLogUrl,
                 String queueName,
                 String callerId,
                 String callerType) {
    this.id = id;
    this.dagId = dagId;
    this.dagName = dagName;
    this.applicationId = applicationId;
    this.initTime = initTime;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
    this.amWebserviceVer = amWebserviceVer;
    this.amLogUrl = amLogUrl;
    this.queueName = queueName;
    this.callerId = callerId;
    this.callerType = callerType;
  }

  @ColumnInfo(columnName="id", exclude = true, id=true)
  public Long id;

  @ColumnInfo(columnName="dag_id", sortable = true, searchable = true)
  private String dagId;

  @ColumnInfo(columnName="dag_name", sortable = true, searchable = true)
  private String dagName;

  @ColumnInfo(columnName="application_id", searchable = true)
  private String applicationId;

  @ColumnInfo(columnName="init_time", fieldName = "dagInitTime")
  private Long initTime;

  @ColumnInfo(columnName="start_time", fieldName = "dagStartTime")
  private Long startTime;

  @ColumnInfo(columnName="end_time", fieldName = "dagEndTime")
  private Long endTime;

  @ColumnInfo(columnName="status", fieldName = "dagStatus")
  private String status;

  @ColumnInfo(columnName="am_webservice_ver", searchable = true)
  private String amWebserviceVer;

  @ColumnInfo(columnName="am_log_url", searchable = true)
  private String amLogUrl;

  @ColumnInfo(columnName="queue_name", sortable = true, searchable = true, fieldName = "dagQueueName")
  private String queueName;

  @ColumnInfo(columnName="caller_id", sortable = true, searchable = true)
  private String callerId;

  @ColumnInfo(columnName="caller_type", sortable = true)
  private String callerType;

  @ColumnInfo(columnName="hive_query_id")
  private Long hiveQueryId;

  @JsonIgnore
  @ColumnInfo(columnName="created_at", exclude = true)
  private LocalDateTime createdAt;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private QueryDetails details;

  @ColumnInfo(columnName="source_file", sortable = true)
  private String sourceFile;

  public static enum Status {
    SUBMITTED, RUNNING, SUCCEEDED, FAILED, KILLED, ERROR
  }
}
