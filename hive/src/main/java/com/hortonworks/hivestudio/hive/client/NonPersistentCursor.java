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


package com.hortonworks.hivestudio.hive.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import com.google.common.collect.Lists;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.actor.message.job.FetchFailed;
import com.hortonworks.hivestudio.hive.actor.message.job.Next;
import com.hortonworks.hivestudio.hive.actor.message.job.NoMoreItems;
import com.hortonworks.hivestudio.hive.actor.message.job.Result;
import com.hortonworks.hivestudio.hive.actor.message.lifecycle.KeepAlive;
import com.hortonworks.hivestudio.hive.utils.HiveActorConfiguration;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.duration.Duration;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wrapper over iterator actor and blocks to fetch Rows and ColumnDescription whenever there is no more Rows to be
 * returned.
 */
@Slf4j
public class NonPersistentCursor implements Cursor<Row, ColumnDescription> {
  private static long DEFAULT_WAIT_TIMEOUT = 60 * 1000L;

  private final ActorSystem system;
  private final ActorRef actorRef;
  private final HiveContext context;
  private final HiveActorConfiguration actorConfiguration;
  private final Queue<Row> rows = Lists.newLinkedList();
  private final List<ColumnDescription> descriptions = Lists.newLinkedList();
  private final Configuration configuration;
  private int offSet = 0;
  private boolean endReached = false;
  private Inbox inbox;
  private Inbox keepAliveInbox;


  public NonPersistentCursor(HiveContext context, Configuration configuration, ActorSystem system, ActorRef actorRef) {
    this.context = context;
    this.system = system;
    this.actorRef = actorRef;
    this.configuration = configuration;
    actorConfiguration = new HiveActorConfiguration(configuration);
    inbox = Inbox.create(system);
    keepAliveInbox = Inbox.create(system);
  }

  @Override
  public boolean isResettable() {
    return false;
  }

  @Override
  public void reset() {
    // Do nothing
  }

  @Override
  public int getOffset() {
    return offSet;
  }

  @Override
  public List<ColumnDescription> getDescriptions() {
    fetchIfNeeded();
    return descriptions;
  }

  @Override
  public void keepAlive() {
    keepAliveInbox.send(actorRef, new KeepAlive());
  }

  @Override
  public Iterator<Row> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    fetchIfNeeded();
    return !endReached;
  }

  @Override
  public Row next() {
    fetchIfNeeded();
    offSet++;
    return rows.poll();
  }

  @Override
  public void remove() {
    throw new RuntimeException("Read only cursor. Method not supported");
  }

  private void fetchIfNeeded() {
    if (endReached || rows.size() > 0) return;
    getNextRows();
  }

  private void getNextRows() {
    inbox.send(actorRef, new Next());
    Object receive;
    try {
      receive = inbox.receive(Duration.create(actorConfiguration.getResultFetchTimeout(DEFAULT_WAIT_TIMEOUT),
        TimeUnit.MILLISECONDS));
    } catch (TimeoutException ex) {
      String errorMessage = "Result fetch timed out";
      log.error(errorMessage, ex);
      throw new ServiceFormattedException(errorMessage, ex);
    }

    if (receive instanceof Result) {
      Result result = (Result) receive;
      if (descriptions.isEmpty()) {
        descriptions.addAll(result.getColumns());
      }
      rows.addAll(result.getRows());
    }

    if (receive instanceof NoMoreItems) {
      if(descriptions.isEmpty()) {
        descriptions.addAll(((NoMoreItems)receive).getColumns());
      }
      endReached = true;
    }

    if (receive instanceof FetchFailed) {
      FetchFailed error = (FetchFailed) receive;
      log.error("Failed to fetch results ");
      throw new ServiceFormattedException(error.getMessage(), error.getError());
    }
  }
}
