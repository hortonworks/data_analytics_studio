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


package com.hortonworks.hivestudio.hive.actor.message;

import com.google.common.base.Optional;
import com.hortonworks.hivestudio.hive.actor.message.job.Failure;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * Message used to send execution complete message.
 * It may contain a ResultSet if the execution returns a ResultSet.
 */
public class ResultInformation {
  /**
   * Execution id to identify the result correspondence of the result with the request
   */
  private final int id;

  /**
   * If the execution returns a ResultSet then this will refer to the ResultSet
   */
  private final ResultSet resultSet;

  private final Failure failure;

  private final boolean cancelled;
  private DatabaseMetaData databaseMetaData;

  private ResultInformation(int id, ResultSet resultSet, Failure failure, boolean cancelled) {
    this.id = id;
    this.resultSet = resultSet;
    this.failure = failure;
    this.cancelled = cancelled;
  }

  public ResultInformation(int id, ResultSet resultSet) {
    this(id, resultSet, null, false);
  }

  public ResultInformation(int id) {
    this(id, null, null, false);
  }

  public ResultInformation(int id, ResultSet resultSet, DatabaseMetaData metaData, Failure failure, boolean cancelled ) {
    this(id, null, null, false);
    this.databaseMetaData = metaData;
  }

  public ResultInformation(int id, Failure failure) {
    this(id, null, failure, false);
  }

  public ResultInformation(int id, boolean cancelled) {
    this(id, null, null, cancelled);
  }

  public ResultInformation(int id, DatabaseMetaData metaData) {
    this(id, null, metaData, null, false);
  }

  public int getId() {
    return id;
  }

  public Optional<ResultSet> getResultSet() {
    return Optional.fromNullable(resultSet);
  }

  public Optional<Failure> getFailure() {
    return Optional.fromNullable(failure);
  }

  public boolean isCancelled() {
    return cancelled;
  }

  public Optional<DatabaseMetaData> getDatabaseMetaData() {
    return Optional.fromNullable(databaseMetaData);
  }

  public void setDatabaseMetaData(DatabaseMetaData databaseMetaData) {
    this.databaseMetaData = databaseMetaData;
  }
}
