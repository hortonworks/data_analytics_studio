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


package com.hortonworks.hivestudio.hive;

import com.google.common.base.Optional;
import com.hortonworks.hivestudio.hive.actor.message.GetColumnMetadataJob;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcConnectionDelegate implements ConnectionDelegate {

  private ResultSet currentResultSet;
  private HiveStatement currentStatement;

  @Override
  public HiveStatement createStatement(HiveConnection connection) throws SQLException {
    Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    currentStatement = (HiveStatement) statement;
    return currentStatement;
  }

  @Override
  public Optional<ResultSet> execute(String statement) throws SQLException {
    if (currentStatement == null) {
      throw new SQLException("Statement not created. Cannot execute Hive queries");
    }

    boolean hasResultSet = currentStatement.execute(statement);

    if (hasResultSet) {
      ResultSet resultSet = currentStatement.getResultSet();
      currentResultSet = resultSet;
      return Optional.of(resultSet);
    } else {
      return Optional.absent();
    }
  }

  @Override
  public Optional<ResultSet> execute(HiveConnection connection, String sqlStatement) throws SQLException {
    createStatement(connection);
    return execute(sqlStatement);
  }


  @Override
  public ResultSet getColumnMetadata(HiveConnection connection, GetColumnMetadataJob job) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns("", job.getSchemaPattern(), job.getTablePattern(), job.getColumnPattern());
    currentResultSet = resultSet;
    return resultSet;
  }

  @Override
  public DatabaseMetaData getDatabaseMetadata(HiveConnection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData;
  }

  @Override
  public void cancel() throws SQLException {
    if (currentStatement != null) {
      currentStatement.cancel();
    }
  }

  @Override
  public void closeResultSet() {

    try {
      if (currentResultSet != null) {
        currentResultSet.close();
      }
    } catch (SQLException e) {
      // Cannot do anything here
    }
  }

  @Override
  public void closeStatement() {
    try {
      if (currentStatement != null) {
        currentStatement.close();
      }
    } catch (SQLException e) {
      // cannot do anything here
    }
  }


}
