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

import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.hive.AuthParams;
import org.apache.hive.jdbc.HiveConnection;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static com.hortonworks.hivestudio.common.Constants.HIVE_SESSION_PARAMS_KEY;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HiveConnectionWrapperTest {

  @Test
  public void testConnectDisconnectIsOpen() throws ConnectionException, SQLException {
    String jdbcUrl = "jdbc://some-url";
    String username = "someUser";
    String password = "somePassword";
    Properties properties = new Properties();
    properties.put(HIVE_SESSION_PARAMS_KEY, "proxyuser=admin");
    AuthParams authParams = new AuthParams(new Configuration(properties));

    HiveConnection mockedConnection = mock(HiveConnection.class);
    when(mockedConnection.isClosed()).thenReturn(false).thenReturn(true);

    HiveConnectionWrapper hiveConnectionWrapper = new HiveConnectionWrapper(jdbcUrl, username, password, authParams){
      public void connect() throws ConnectionException {
        this.connection = mockedConnection;
      }
    };
    hiveConnectionWrapper.connect();

    Assert.assertTrue(hiveConnectionWrapper.isOpen());

    hiveConnectionWrapper.disconnect();
    verify(mockedConnection, times(1)).close();

    Assert.assertFalse(hiveConnectionWrapper.isOpen());

    verify(mockedConnection, times(0)).isValid(anyInt());
    verify(mockedConnection, times(2)).isClosed();
  }
}