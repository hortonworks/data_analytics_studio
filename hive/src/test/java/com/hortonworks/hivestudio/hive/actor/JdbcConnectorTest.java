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

import static com.hortonworks.hivestudio.common.Constants.HIVE_SESSION_PARAMS_KEY;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.util.Properties;

import javax.inject.Provider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.hive.AuthParams;
import com.hortonworks.hivestudio.hive.ConnectionDelegate;
import com.hortonworks.hivestudio.hive.actor.message.Connect;
import com.hortonworks.hivestudio.hive.internal.Connectable;
import com.hortonworks.hivestudio.hive.persistence.repositories.JobRepository;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;

public class JdbcConnectorTest {
  ActorSystem actorSystem = null;
  TestActorRef<JdbcConnector> jdbcConnectorRef;
  JdbcConnector jdbcConnector;
  private Connectable mockConnectable = null;

  @Before
  public void setup(){
    actorSystem = ActorSystem.create("testactorSystem");
    Properties properties = new Properties();
    properties.put(HIVE_SESSION_PARAMS_KEY, "proxyuser=admin");
    Configuration configuration = new Configuration(properties);

    ActorRef parent = null;
    HdfsApi hdfsApi = null;
    ConnectionDelegate connectionDelegate = null;
    mockConnectable = mock(Connectable.class);
    Provider < JobRepository > storage = null;
    final Props props = Props.create(JdbcConnector.class, () -> new JdbcConnector(
        configuration, parent, hdfsApi, connectionDelegate, storage) {
      @Override
      protected void stopInactivityScheduler() {
        // cannot be tested
      }
      @Override
      protected void stopTerminateInactivityScheduler() {
        // cannot be tested
      }
      @Override
      protected void startTerminateInactivityScheduler(){
      }
      @Override
      protected void startInactivityScheduler() {
      }
      @Override
      protected Connectable getConnectable(Connect connect, AuthParams authParams){
        return mockConnectable;
      }
    });
    jdbcConnectorRef = TestActorRef.create(actorSystem, props, "JdbcConnectorTest");
    jdbcConnector = jdbcConnectorRef.underlyingActor();
  }

  @After
  public void tearDown(){
    actorSystem.terminate();
  }

  @Test
  public void testPostStop() throws Exception {
    Connect connectMsg = new Connect(null, null, null, null);
    jdbcConnectorRef.tell(connectMsg, ActorRef.noSender());

    jdbcConnector.postStop();
    //create an inOrder verifier for a single mock
    InOrder inOrder = inOrder(mockConnectable);

    inOrder.verify(mockConnectable, times(1)).isOpen();
    inOrder.verify(mockConnectable, times(1)).connect();
    inOrder.verify(mockConnectable, times(0)).isOpen();
    inOrder.verify(mockConnectable, times(1)).disconnect();
  }
}