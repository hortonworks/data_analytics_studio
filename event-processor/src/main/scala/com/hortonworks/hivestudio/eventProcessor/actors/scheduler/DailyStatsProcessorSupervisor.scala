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

import javax.inject.{Inject, Provider}

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy}
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension
import com.hortonworks.hivestudio.eventProcessor.actors.scheduler.DBAuditorActor._
import com.hortonworks.hivestudio.eventProcessor.actors.scheduler.DBQueryReader.{QueriesToProcess, ReadQueries}
import com.hortonworks.hivestudio.eventProcessor.actors.scheduler.ReportDataAccumulator.{ProcessQueries, ProcessingSucceeded}
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType

import scala.concurrent.duration.FiniteDuration

object DailyStatsProcessorSupervisor {
  case object CreateNewStats
}


class DailyStatsProcessorSupervisor @Inject()(extensionProvider: Provider[GuiceAkkaExtension.AkkaExtensionProvider]) extends Actor with ActorLogging {
  import DailyStatsProcessorSupervisor._

  import scala.concurrent.ExecutionContext.Implicits.global

  var dBAuditorActor: ActorRef = _

  val extension = extensionProvider.get()
  val queriesReader: ActorRef = context.actorOf(extension.props(classOf[DBQueryReader]), "QueriesReader")
  val dataAccumulator: ActorRef = context.actorOf(extension.props(classOf[ReportDataAccumulator]), "ReportDataAccumulator")

  override def receive = {
    case CreateNewStats =>
      log.info(s"Creating new stats for reading. DB Auditor value: ${dBAuditorActor.path}")
      dBAuditorActor ! NextRunInfo(SchedulerAuditType.DAILY_ROLLUP)
      context.become(awaitForNextRead)

    case InitializeAuditor(actor) =>
      log.info(s"Auditor initializes: ${actor.path}")
      dBAuditorActor = actor

    case NextRunInfo =>
      dBAuditorActor ! NextRunInfo(SchedulerAuditType.DAILY_ROLLUP)

    case ProcessingSucceeded(hiveIds) =>
      dBAuditorActor ! StatsProcessingSucceeded(SchedulerAuditType.DAILY_ROLLUP)
  }



  def awaitForNextRead: Receive = {
    case RunInfo(_, _, x, SchedulerAuditType.DAILY_ROLLUP) =>
      log.info("Reading the next set of queries for stats processing")
      queriesReader ! ReadQueries(x)
      context.become(awaitForStatsProcessing)
    case RetryAfter(duration) =>
      retryAgainAfter(duration)
      context.become(receive)

  }

  def awaitForStatsProcessing: Receive = {
    case QueriesToProcess(explainPlans) =>
      log.info(s"Explain plans received, ${explainPlans.size}")
      if(explainPlans.isEmpty) {
        log.info("No queries to process. Skipping this run.")
        dBAuditorActor ! StatsProcessingSucceeded(SchedulerAuditType.DAILY_ROLLUP)
      } else {
        dBAuditorActor ! UpdateHiveIds(explainPlans.map(_.getHiveId().asInstanceOf[Long]).sorted)
        dataAccumulator ! ProcessQueries(explainPlans)
      }
      context.become(receive)


  }

  def retryAgainAfter(duration: FiniteDuration): Unit = {
    context.system.scheduler.scheduleOnce(duration, self, NextRunInfo)
  }

  override def supervisorStrategy =
    AllForOneStrategy() {
      case _: Exception => Escalate
    }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    dBAuditorActor ! StatsProcessingFailed(self, reason, message, SchedulerAuditType.DAILY_ROLLUP)
  }
}
