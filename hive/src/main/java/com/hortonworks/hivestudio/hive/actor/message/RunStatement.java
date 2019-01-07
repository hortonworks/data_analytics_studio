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
import lombok.Value;

/**
 * Message sent by JdbcConnector to StatementExecutor to run a statement
 */
public class RunStatement {
  /**
   * This is the execution id meant to identify the executing statement sequence
   */
  private final int id;
  private final String statement;
  private final String logFile;
  private final Integer jobId;
  private final boolean startLogAggregation;
  private final boolean startGUIDFetch;

  public RunStatement(int id, String statement, Integer jobId, boolean startLogAggregation, String logFile, boolean startGUIDFetch) {
    this.id = id;
    this.statement = statement;
    this.jobId = jobId;
    this.logFile = logFile;
    this.startLogAggregation = startLogAggregation;
    this.startGUIDFetch = startGUIDFetch;
  }

  public RunStatement(int id, String statement) {
    this(id, statement, null, false, null, false);
  }

  public int getId() {
    return id;
  }

  public String getStatement() {
    return statement;
  }

  public Optional<String> getLogFile() {
    return Optional.fromNullable(logFile);
  }

  public boolean shouldStartLogAggregation() {
    return startLogAggregation;
  }

  public boolean shouldStartGUIDFetch() {
    return startGUIDFetch;
  }

  public Optional<Integer> getJobId() {
    return Optional.fromNullable(jobId);
  }
}
