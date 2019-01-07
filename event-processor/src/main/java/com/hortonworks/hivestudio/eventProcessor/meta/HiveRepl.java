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
package com.hortonworks.hivestudio.eventProcessor.meta;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;

import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpInfo;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.services.ConnectionFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveRepl {
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String FULLDUMP_SQL = "repl dump `*` with " +
      "('hive.repl.dump.metadata.only'='true', 'hive.repl.dump.include.acid.tables'='true')";
  private static final String INCREMENTAL_SQL = "repl dump `*` from %s with " +
      "('hive.repl.dump.metadata.only'='true', 'hive.repl.dump.include.acid.tables'='true')";

  private final ConnectionFactory connectionFactory;
  private final AppAuthentication appAuth;

  @Inject
  public HiveRepl(ConnectionFactory connectionFactory, AppAuthentication appAuth) {
    this.connectionFactory = connectionFactory;
    this.appAuth = appAuth;
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot load driver class: " + DRIVER_NAME);
    }
  }

  private WarehouseDumpInfo getDumpWithSql(String sql) {
    HiveContext context = new HiveContext(appAuth.getAppUser());
    String jdbcUrl = connectionFactory.createJdbcUrl(context);
    jdbcUrl = connectionFactory.appendSessionParams(context, jdbcUrl);
    log.debug("Connecting to jdbc url: {}", jdbcUrl);
    try (HiveConnection connection = (HiveConnection) DriverManager.getConnection(jdbcUrl);
        HiveStatement statement = (HiveStatement) connection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      log.info("Executing sql: " + sql);
      if (statement.execute(sql)) {
        try (ResultSet resultSet = statement.getResultSet()) {
          if (resultSet.next()) {
            String dumpPath = resultSet.getString(1);
            String lastReplicationId = resultSet.getString(2);
            log.info("Got result for sql: {}, dumpPath: {}, replId: {}", sql, dumpPath,
                lastReplicationId);
            return new WarehouseDumpInfo(dumpPath, lastReplicationId);
          }
        }
      }
      throw new RuntimeException("No results found");
    } catch (SQLException e) {
      throw new RuntimeException("Error occured while trying to repl dump: ", e);
    }
  }

  public WarehouseDumpInfo getWarehouseBootstrapDump() {
    return getDumpWithSql(FULLDUMP_SQL);
  }

  public WarehouseDumpInfo getWarehouseIncrementalDump(String lastReplicationId) {
    return getDumpWithSql(String.format(INCREMENTAL_SQL, lastReplicationId));
  }
}
