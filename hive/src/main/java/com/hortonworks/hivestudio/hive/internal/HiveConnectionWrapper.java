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


package com.hortonworks.hivestudio.hive.internal;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.HiveConnection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.hortonworks.hivestudio.hive.AuthParams;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Composition over a Hive jdbc connection
 * This class only provides a connection over which
 * callers should run their own JDBC statements
 */
@ToString
@Slf4j
public class HiveConnectionWrapper implements Connectable, Supplier<HiveConnection> {

  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  public static final String SUFFIX = "validating the login";
  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final AuthParams authParams;

  private UserGroupInformation ugi;

  @VisibleForTesting
  protected HiveConnection connection = null;
  private boolean authFailed;

  public HiveConnectionWrapper(String jdbcUrl, String username, String password, AuthParams authParams) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.authParams = authParams;
  }

  @Override
  public void connect() throws ConnectionException {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      throw new ConnectionException(e, "Cannot load the hive JDBC driver");
    }

    try {
      connection = (HiveConnection) DriverManager.getConnection(jdbcUrl, username, password);
      log.info("Successfully created new hive connection: {}", connection);
    } catch (UndeclaredThrowableException exception) {
      // Check if the reason was an auth error
      Throwable cause = exception.getCause();
      if (cause instanceof SQLException && isLoginError((SQLException)cause)) {
        authFailed = true;
      }
      throw new ConnectionException(cause,
          "Cannot open a hive connection with connect string " + jdbcUrl);
    } catch (SQLException e) {
      throw new ConnectionException(e,
          "Cannot open a hive connection with connect string " + jdbcUrl);
    }
  }

  @Override
  public void reconnect() throws ConnectionException {

  }

  @Override
  public void disconnect() throws ConnectionException {
    if (connection != null) {
      try {
        connection.close();
        log.info("Successfully closed hive connection: {}", connection);
      } catch (SQLException e) {
        throw new ConnectionException(e, "Cannot close the hive connection with connect string " + jdbcUrl);
      }
    }
  }

  private boolean isLoginError(SQLException ce) {
    return ce.getCause().getMessage().toLowerCase().endsWith(SUFFIX);
  }

  /**
   * True when the connection is unauthorized
   *
   * @return
   */
  @Override
  public boolean isUnauthorized() {
    return authFailed;
  }

  public Optional<HiveConnection> getConnection() {
    return Optional.fromNullable(connection);
  }

  @Override
  public boolean isOpen() throws ConnectionException{
    try {
      return connection != null && !connection.isClosed();
    } catch (SQLException e) {
      log.error("Error occurred while checking if hive connection is open. Returning false.", e);
      throw new ConnectionException(e, e.getMessage());
    }
  }

  /**
   * Retrieves an instance of the appropriate type. The returned object may or
   * may not be a new instance, depending on the implementation.
   *
   * @return an instance of the appropriate type
   */
  @Override
  public HiveConnection get() {
    return null;
  }
}
