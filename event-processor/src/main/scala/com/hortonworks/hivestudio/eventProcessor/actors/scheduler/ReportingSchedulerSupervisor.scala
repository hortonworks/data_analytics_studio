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

import akka.actor.SupervisorStrategy._
import akka.actor.{Actor, ActorLogging, Cancellable, OneForOneStrategy}
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension
import com.hortonworks.hivestudio.eventProcessor.actors.scheduler.DBAuditorActor.InitializeAuditor
import com.hortonworks.hivestudio.eventProcessor.actors.scheduler.DailyStatsProcessorSupervisor.CreateNewStats
import com.hortonworks.hivestudio.eventProcessor.configuration.EventProcessingConfig
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType

object ReportingSchedulerSupervisor {

  case object Start

  case object Stop

  case object DailyTick

  case object WeeklyTick

  case object MonthlyTick

  case object QuarterlyTick

  val INITIAL_DELAY_CONFIG_NAME = "reporting.scheduler.initial.delay.millis"
  val INTERVAL_DELAY_CONFIG_NAME = "reporting.scheduler.interval.delay.millis"
  val INITIAL_WEEKLY_DELAY_CONFIG_NAME = "reporting.scheduler.weekly.initial.delay.millis"
  val INTERVAL_WEEKLY_DELAY_CONFIG_NAME = "reporting.scheduler.weekly.interval.delay.millis"
  val INITIAL_MONTHLY_DELAY_CONFIG_NAME = "reporting.scheduler.monthly.initial.delay.millis"
  val INTERVAL_MONTHLY_DELAY_CONFIG_NAME = "reporting.scheduler.monthly.interval.delay.millis"
  val INITIAL_QUARTERLY_DELAY_CONFIG_NAME = "reporting.scheduler.quarterly.initial.delay.millis"
  val INTERVAL_QUARTERLY_DELAY_CONFIG_NAME = "reporting.scheduler.quarterly.interval.delay.millis"
}

/**
  * Supervisor to start the Reporting scheduler pipeline
  */
class ReportingSchedulerSupervisor @Inject()(extensionProvider: Provider[GuiceAkkaExtension.AkkaExtensionProvider], configuration: EventProcessingConfig) extends Actor with ActorLogging {

  import ReportingSchedulerSupervisor._
  import RollupStatsProcessor._

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  var cancellable: Cancellable = _
  var cancellableWeekly: Cancellable = _
  var cancellableMonthly: Cancellable = _
  var cancellableQuarterly: Cancellable = _

  val extension = extensionProvider.get()

  val auditorRef = context.actorOf(extension.props(classOf[DBAuditorActor]), "ReportingDBAuditor")
  val statsProcessorSupervisor = context.actorOf(extension.props(classOf[DailyStatsProcessorSupervisor]), "DailyStatsProcessorSupervisor")
  val weeklyStatsProcessorSupervisor = context.actorOf(extension.props(classOf[RollupStatsProcessor]), "WeeklyStatsProcessorSupervisor")
  val monthlyStatsProcessorSupervisor = context.actorOf(extension.props(classOf[RollupStatsProcessor]), "MonthlyStatsProcessorSupervisor")
  val quarterlyStatsProcessorSupervisor = context.actorOf(extension.props(classOf[RollupStatsProcessor]), "QuarterlyStatsProcessorSupervisor")

  statsProcessorSupervisor ! InitializeAuditor(auditorRef)
  weeklyStatsProcessorSupervisor ! InitializeAuditor(auditorRef)
  monthlyStatsProcessorSupervisor ! InitializeAuditor(auditorRef)
  quarterlyStatsProcessorSupervisor ! InitializeAuditor(auditorRef)

  override def receive = {
    case Start =>
      startPipeline
    case Stop =>
      stopPipeline
    case DailyTick =>
      statsProcessorSupervisor ! CreateNewStats

    case WeeklyTick =>
      weeklyStatsProcessorSupervisor ! CreateOrUpdateStats(SchedulerAuditType.WEEKLY_ROLLUP)
    case MonthlyTick =>
      monthlyStatsProcessorSupervisor ! CreateOrUpdateStats(SchedulerAuditType.MONTHLY_ROLLUP)
    case QuarterlyTick =>
      quarterlyStatsProcessorSupervisor ! CreateOrUpdateStats(SchedulerAuditType.QUARTERLY_ROLLUP)
  }

  def startPipeline = {
    log.info("Starting pipeline to prepare and update reporting data at a fixed interval")
    startTicker
  }

  def stopPipeline = {
    log.info("Stopping pipeline for reporting data preparation and update")
  }

  def startTicker = {
    val initialDelay = configuration.getAsLong(INITIAL_DELAY_CONFIG_NAME, 30000)
    val interval = configuration.getAsLong(INTERVAL_DELAY_CONFIG_NAME, 300000)
    cancellable = context.system.scheduler.schedule(FiniteDuration(initialDelay, MILLISECONDS),
      FiniteDuration(interval, MILLISECONDS), self, DailyTick)

    val initialWeeklyDelay = configuration.getAsLong(INITIAL_WEEKLY_DELAY_CONFIG_NAME, 60000)
    val weeklyInterval = configuration.getAsLong(INTERVAL_WEEKLY_DELAY_CONFIG_NAME, 90000)
    val initialMonthlyDelay = configuration.getAsLong(INITIAL_MONTHLY_DELAY_CONFIG_NAME, 90000)
    val monthlyInterval = configuration.getAsLong(INTERVAL_MONTHLY_DELAY_CONFIG_NAME, 120000)
    val initialQuarterlyDelay = configuration.getAsLong(INITIAL_QUARTERLY_DELAY_CONFIG_NAME, 120000)
    val quarterlyInterval = configuration.getAsLong(INTERVAL_QUARTERLY_DELAY_CONFIG_NAME, 150000)


    cancellableWeekly = context.system.scheduler.schedule(FiniteDuration(initialWeeklyDelay, MILLISECONDS),
      FiniteDuration(weeklyInterval, MILLISECONDS), self, WeeklyTick)

    cancellableMonthly = context.system.scheduler.schedule(FiniteDuration(initialMonthlyDelay, MILLISECONDS),
      FiniteDuration(monthlyInterval, MILLISECONDS), self, MonthlyTick)

    cancellableQuarterly = context.system.scheduler.schedule(FiniteDuration(initialQuarterlyDelay, MILLISECONDS),
      FiniteDuration(quarterlyInterval, MILLISECONDS), self, QuarterlyTick)
  }


  override def supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: Exception => Restart
    }

  override def postStop(): Unit = {
    super.postStop()
    log.error("stopping Reporting scheduler supervisor")
  }
}
