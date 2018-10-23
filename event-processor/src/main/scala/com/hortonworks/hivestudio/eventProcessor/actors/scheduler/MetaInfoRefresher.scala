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
package com.hortonworks.hivestudio.eventProcessor.actors.scheduler

import java.util.concurrent.TimeUnit
import javax.inject.{Inject, Provider}

import akka.actor.{Actor, ActorLogging, Stash, Timers}
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository
import com.hortonworks.hivestudio.eventProcessor.configuration.Constants
import com.hortonworks.hivestudio.eventProcessor.meta._

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager


object MetaInfoRefresher {

  case class Refresh()

  case class Init(refreshDelay: Long)

  private val DB_REFRESH_KEY = "RefreshKey"
}

class MetaInfoRefresher @Inject()(metaInfoUpdater: MetaInfoUpdater,
    dBReplicationRepository: Provider[DBReplicationRepository], mgr: TransactionManager)
  extends Actor with Timers with Stash with ActorLogging {

  import MetaInfoRefresher._

  var init: Init = _

  // init
  private var refreshDelay: Long = 0L

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info("inside preRestart Sending Init message to self : {}", init)
    self ! init
  }

  def becomeInitialized(): Unit = {
    unstashAll()
    context.become(initialized)
  }

  private def initialize(init: Init): Unit = {
    this.init = init
    this.refreshDelay = init.refreshDelay
    scheduleRefresh()
    becomeInitialized()
  }

  private def initing: Receive = {
    case init: Init =>
      log.debug("received init : {} ", init)
      initialize(init)

    case msg => stash();
  }

  private def initialized: Receive = {
    case refresh: Refresh =>
      log.debug("received refresh ")
      this.refresh()
  }


  override def receive: Receive = initing

  private def scheduleRefresh(): Unit = {
    log.info("scheduleRefresh for metadata after delay : {} ms.", refreshDelay)
    timers.startSingleTimer(MetaInfoRefresher.DB_REFRESH_KEY, Refresh(), Duration.create(refreshDelay, TimeUnit.MILLISECONDS))
  }

  private def refresh(): Unit = {
    val successfull: Boolean = mgr.withTransaction(() => refreshMeta)
    log.info("Was refresh meta successfull : {}", successfull)
    scheduleRefresh()
  }


  private def refreshMeta: Boolean = {
    try {
      var replicationEntity = dBReplicationRepository.get().getByDatabaseName(Constants.ALL_DB_STAR).asScala
      // update the list of dbs
      // it is important to maintain the order of fetching the list of DBs first from HS and then hive-server
      // to avoid a inconsistent drop of database entry in HS
      var success: Boolean = true
      val startTime = System.currentTimeMillis()
      replicationEntity match {
        case Some(entity) =>
          metaInfoUpdater.updateWarehouseFromDump(entity)
        case None =>
          metaInfoUpdater.bootstrapWarehouseFromDump()
      }

      val afterDBsSyncedTime = System.currentTimeMillis()
      log.info("total time taken to update meta data : {}", (afterDBsSyncedTime - startTime))
      success
    } catch {
      case e: Exception =>
        log.error("Error while syncing metadata using replication. ", e)
        false
    }
  }
}
