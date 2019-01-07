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


package com.hortonworks.hivestudio.hive.actor;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;
import com.hortonworks.hivestudio.hive.actor.message.CursorReset;
import com.hortonworks.hivestudio.hive.actor.message.HiveMessage;
import com.hortonworks.hivestudio.hive.actor.message.ResetCursor;
import com.hortonworks.hivestudio.hive.actor.message.job.FetchFailed;
import com.hortonworks.hivestudio.hive.actor.message.job.Next;
import com.hortonworks.hivestudio.hive.actor.message.job.NoMoreItems;
import com.hortonworks.hivestudio.hive.actor.message.job.Result;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.CleanUp;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.KeepAlive;
import com.hortonworks.hivestudio.hive.client.ColumnDescription;
import com.hortonworks.hivestudio.hive.client.ColumnDescriptionShort;
import com.hortonworks.hivestudio.hive.client.Row;

import akka.actor.ActorRef;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResultSetIterator extends HiveActor {
  private static final int DEFAULT_BATCH_SIZE = 100;
  public static final String NULL = "NULL";

  private final ActorRef parent;
  private final ResultSet resultSet;
  private final int batchSize;

  private List<ColumnDescription> columnDescriptions;
  private int columnCount;
  boolean async = false;
  private boolean metaDataFetched = false;

  public ResultSetIterator(ActorRef parent, ResultSet resultSet, int batchSize, boolean isAsync) {
    this.parent = parent;
    this.resultSet = resultSet;
    this.batchSize = batchSize;
    this.async = isAsync;
  }

  public ResultSetIterator(ActorRef parent, ResultSet resultSet) {
    this(parent, resultSet, DEFAULT_BATCH_SIZE, true);
  }

  public ResultSetIterator(ActorRef parent, ResultSet resultSet, boolean isAsync) {
    this(parent, resultSet, DEFAULT_BATCH_SIZE, isAsync);
  }

  @Override
  public void handleMessage(HiveMessage hiveMessage) {
    sendKeepAlive();
    Object message = hiveMessage.getMessage();
    if (message instanceof Next) {
      getNext();
    }
    if (message instanceof ResetCursor) {
      resetResultSet();
    }

    if (message instanceof KeepAlive) {
      sendKeepAlive();
    }
  }

  private void resetResultSet() {
    try {
      resultSet.beforeFirst();
      sender().tell(new CursorReset(), self());
    } catch (SQLException e) {
      log.error("Failed to reset the cursor", e);
      sender().tell(new FetchFailed("Failed to reset the cursor", e), self());
      cleanUpResources();
    }
  }

  private void sendKeepAlive() {
    log.debug("Sending a keep alive to {}", parent);
    parent.tell(new KeepAlive(), self());
  }

  private void getNext() {
    List<Row> rows = Lists.newArrayList();
    if (!metaDataFetched) {
      try {
        initialize();
      } catch (SQLException ex) {
        log.error("Failed to fetch metadata for the ResultSet", ex);
        sender().tell(new FetchFailed("Failed to get metadata for ResultSet", ex), self());
        cleanUpResources();
      }
    }
    int index = 0;
    try {
      // check batchsize first becaue resultSet.next() fetches the new row as well before returning true/false.
      while (index < batchSize && resultSet.next()) {
        index++;
        rows.add(getRowFromResultSet(resultSet));
      }

      if (index == 0) {
        // We have hit end of resultSet
        sender().tell(new NoMoreItems(columnDescriptions), self());
        if(!async) {
          cleanUpResources();
        }
      } else {
        Result result = new Result(rows, columnDescriptions);
        sender().tell(result, self());
      }

    } catch (SQLException ex) {
      log.error("Failed to fetch next batch for the Resultset", ex);
      sender().tell(new FetchFailed("Failed to fetch next batch for the Resultset", ex), self());
      cleanUpResources();
    }
  }

  private void cleanUpResources() {
    parent.tell(new CleanUp(), self());
  }

  private Row getRowFromResultSet(ResultSet resultSet) throws SQLException {
    Object[] values = new Object[columnCount];
    for (int i = 0; i < columnCount; i++) {
      values[i] = resultSet.getObject(i + 1);
    }
    return new Row(values);
  }

  private void initialize() throws SQLException {
    metaDataFetched = true;
    ResultSetMetaData metaData = resultSet.getMetaData();
    columnCount = metaData.getColumnCount();
    columnDescriptions = Lists.newArrayList();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      String typeName;
      try {
        typeName = metaData.getColumnTypeName(i);
      } catch (SQLException e) {
        if (e.getMessage().contains("UNIONTYPE")) {
          typeName = "uniontype";
        } else {
          throw e;
        }
      }
      ColumnDescription description = new ColumnDescriptionShort(columnName, typeName, i);
      columnDescriptions.add(description);
    }
  }
}
