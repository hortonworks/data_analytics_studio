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

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import javax.inject.{Inject, Provider}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension
import com.hortonworks.hivestudio.eventProcessor.actors.scheduler.DBAuditorActor._
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType
import com.hortonworks.hivestudio.eventProcessor.processors.stats.{ColumnStatsProcessor, JoinStatsProcessor, TableStatsProcessor}

import scala.concurrent.duration.FiniteDuration
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager

object RollupStatsProcessor {

  case class CreateOrUpdateStats(scheduleType: SchedulerAuditType)

}

class RollupStatsProcessor @Inject()(tableStatsProcessor: TableStatsProcessor,
                                     columnStatsProcessor: ColumnStatsProcessor,
                                     joinStatsProcessor: JoinStatsProcessor,
                                     mgr: TransactionManager,
                                     extensionProvider: Provider[GuiceAkkaExtension.AkkaExtensionProvider]) extends Actor with ActorLogging {

  import RollupStatsProcessor._

  import scala.concurrent.ExecutionContext.Implicits.global

  var dBAuditorActor: ActorRef = _
  var schedulerType: SchedulerAuditType = _

  val extension = extensionProvider.get()

  override def receive = {
    case CreateOrUpdateStats(x) =>
      log.info(s"Creating new stats for reading. DB Auditor value: ${dBAuditorActor.path}")
      schedulerType = x
      dBAuditorActor ! LastRunInfo(x)
      context.become(awaitForLastRunInfo)

    case InitializeAuditor(actor) =>
      log.info(s"Auditor initializes: ${actor.path}")
      dBAuditorActor = actor

    case NextRunInfo(x) =>
      schedulerType = x
      dBAuditorActor ! LastRunInfo(x)
      context.become(awaitForLastRunInfo)
  }


  def awaitForLastRunInfo: Receive = {
    case RunInfo(_, date, _, sType) =>
      log.info("Reading the next set of queries for stats processing")
      checkLastRun(Some(date), sType)
      context.become(receive)
    case EmptyRunInfo(sType) =>
      checkLastRun(None, sType)
      context.become(receive)
    case RetryAfter(duration) =>
      retryAgainAfter(duration)
      context.become(receive)




  }


  def checkLastRun(date: Option[LocalDate], sType: SchedulerAuditType) = {
    date match {
      case None =>
        processForDate(LocalDate.now(), sType)
        log.info(s"No last run entry for $sType, finalizing for today.")
      case Some(x) =>
        if(x.isBefore(LocalDate.now())) {
          log.info(s"Last run for $sType was for some previous day, finalizing for all days in between $date and now.")
          processFromDate(x, sType)
        } else {
          log.info(s"Last run for $sType was for today, finalizing for today.")
          processForDate(LocalDate.now(), sType)
        }
    }
    dBAuditorActor ! StatsProcessingSucceeded(schedulerType)
  }

  def processForDate(date: LocalDate, sType: SchedulerAuditType) = {
    log.info(s"Processing date for $sType for date $date")
    mgr.withTransaction(() => {
      tableStatsProcessor.rollupCounts(date, sType)
      columnStatsProcessor.rollupCounts(date, sType)
      joinStatsProcessor.rollupCounts(date, sType)
    })
  }

  /**
    * Finalized from the date given till yesterday.
    */
  def processFromDate(date: LocalDate, sType: SchedulerAuditType) = {
    val numberOfDaysBetween = ChronoUnit.DAYS.between(date, LocalDate.now())
    (0L to numberOfDaysBetween).foreach(x => processForDate(date.plusDays(x), sType))
  }


  def checkEmptyLastRun(sType: SchedulerAuditType): Unit = {
    log.info(s"No entries found in the Scheduler Audit for schedulerType $sType.")

  }

  def checkNonEmptyLastRun(date: LocalDate): Unit = {

  }

  def retryAgainAfter(duration: FiniteDuration): Unit = {
    context.system.scheduler.scheduleOnce(duration, self, NextRunInfo(schedulerType))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    dBAuditorActor ! StatsProcessingFailed(self, reason, message, schedulerType)
  }
}

