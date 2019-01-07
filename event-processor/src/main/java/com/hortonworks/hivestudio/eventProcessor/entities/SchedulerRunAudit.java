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
package com.hortonworks.hivestudio.eventProcessor.entities;

import java.time.LocalDateTime;

import com.hortonworks.hivestudio.common.repository.Identifiable;

import lombok.Data;

@Data
public class SchedulerRunAudit implements Identifiable<Integer> {

  private Integer id;

  private SchedulerAuditType type;

  private LocalDateTime readStartTime;
  private LocalDateTime readEndTime;

  private String queriesProcessed;
  private String status;
  private String failureReason;

  private Integer lastTryId;
  private Integer retryCount;

  @Override
  public Integer getId() {
    return id;
  }

  @Override
  public void setId(Integer id) {
    this.id = id;
  }

  public SchedulerAuditType getType() {
    return type;
  }

  public void setType(SchedulerAuditType type) {
    this.type = type;
  }

  public LocalDateTime getReadStartTime() {
    return readStartTime;
  }

  public void setReadStartTime(LocalDateTime readStartTime) {
    this.readStartTime = readStartTime;
  }

  public LocalDateTime getReadEndTime() {
    return readEndTime;
  }

  public void setReadEndTime(LocalDateTime readEndTime) {
    this.readEndTime = readEndTime;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getFailureReason() {
    return failureReason;
  }

  public void setFailureReason(String failureReason) {
    this.failureReason = failureReason;
  }

  public Integer getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(Integer retryCount) {
    this.retryCount = retryCount;
  }

  public String getQueriesProcessed() {
    return queriesProcessed;
  }

  public void setQueriesProcessed(String queriesProcessed) {
    this.queriesProcessed = queriesProcessed;
  }

  public Integer getLastTryId() {
    return lastTryId;
  }

  public void setLastTryId(Integer lastTryId) {
    this.lastTryId = lastTryId;
  }

}
