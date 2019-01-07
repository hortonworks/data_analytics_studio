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
package com.hortonworks.hivestudio.hive.persistence.entities;

import com.hortonworks.hivestudio.common.repository.Identifiable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Bean to represent a job
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Job implements Identifiable<Integer>, IJob {

  public enum REFERRER {
    INTERNAL,
    USER
  }

  public static String JOB_STATE_UNKNOWN = "UNKNOWN";
  public static String JOB_STATE_INITIALIZED = "INITIALIZED";
  public static String JOB_STATE_RUNNING = "RUNNING";
  public static String JOB_STATE_FINISHED = "SUCCEEDED";
  public static String JOB_STATE_CANCELED = "CANCELED";
  public static String JOB_STATE_CLOSED = "CLOSED";
  public static String JOB_STATE_ERROR = "ERROR";
  public static String JOB_STATE_PENDING = "PENDING";

  private Integer id = null;
  private String owner = null;

  private String title = null;
  private String statusDir = null;
  private Long dateSubmitted = 0L;
  private Long duration = 0L;

  private String query = null;
  private String selectedDatabase = null;

  private String status = JOB_STATE_UNKNOWN;
  private String referrer;
  private String globalSettings;
  private String logFile;
  private String guid = null;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Job)) return false;

    Job job = (Job) o;

    return id != null ? id.equals(job.id) : job.id == null;

  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }

  @Override
  public Integer getId() {
    return id;
  }

  @Override
  public void setId(Integer id) {
    this.id = id;
  }

  @Override
  public String getOwner() {
    return owner;
  }

  @Override
  public void setOwner(String owner) {
    this.owner = owner;
  }

  @Override
  public String getTitle() {
    return title;
  }

  @Override
  public void setTitle(String title) {
    this.title = title;
  }

  @Override
  public Long getDateSubmitted() {
    return dateSubmitted;
  }

  @Override
  public void setDateSubmitted(Long dateSubmitted) {
    this.dateSubmitted = dateSubmitted;
  }

  @Override
  public Long getDuration() {
    return duration;
  }

  @Override
  public void setDuration(Long duration) {
    this.duration = duration;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public void setQuery(String query) {
    this.query = query;
  }

  @Override
  public String getStatusDir() {
    return statusDir;
  }

  @Override
  public void setStatusDir(String statusDir) {
    this.statusDir = statusDir;
  }

  @Override
  public String getSelectedDatabase() {
    return selectedDatabase;
  }

  @Override
  public void setSelectedDatabase(String selectedDatabase) {
    this.selectedDatabase = selectedDatabase;
  }

  @Override
  public String getLogFile() {
    return logFile;
  }

  @Override
  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  @Override
  public String getReferrer() {
    return referrer;
  }

  @Override
  public void setReferrer(String referrer) {
    this.referrer = referrer;
  }

  @Override
  public String getGlobalSettings() {
    return globalSettings;
  }

  @Override
  public void setGlobalSettings(String globalSettings) {
    this.globalSettings = globalSettings;
  }

  @Override
  public String getGuid() {
    return guid;
  }

  @Override
  public void setGuid(String guid) {
    this.guid = guid;
  }
}
