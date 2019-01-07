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
import javax.inject.Inject

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.hortonworks.hivestudio.eventProcessor.entities.{SchedulerAuditType, SchedulerRunAudit}
import com.hortonworks.hivestudio.eventProcessor.services.AuditDataService

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager

object DBAuditorActor {

  case class StatsProcessingFailed(statsProcessorRef: ActorRef, reason: Throwable, message: Option[Any], schedulerType: SchedulerAuditType)

  case class StatsProcessingSucceeded(schedulerType: SchedulerAuditType)

  case class LastRunInfo(schedulerType: SchedulerAuditType)

  case class NextRunInfo(schedulerType: SchedulerAuditType)

  case class RunInfo(auditId: Int, date: LocalDate, entriesToProcess: Option[Seq[Long]] = None, schedulerAuditType: SchedulerAuditType = SchedulerAuditType.DAILY_ROLLUP)

  case class EmptyRunInfo(schedulerAuditType: SchedulerAuditType = SchedulerAuditType.DAILY_ROLLUP)

  case class RetryAfter(after: FiniteDuration)

  case class UpdateHiveIds(hiveIds: Seq[Long])

  case class InitializeAuditor(auditor: ActorRef)

}

class DBAuditorActor @Inject()(auditService: AuditDataService, mgr: TransactionManager) extends Actor with ActorLogging {

  import DBAuditorActor._
  import DailyStatsProcessorSupervisor._

  import scala.compat.java8.OptionConverters._
  import scala.concurrent.duration._

  var currentRunInfoMap: mutable.Map[SchedulerAuditType, Option[RunInfo]] = mutable.HashMap(
    SchedulerAuditType.DAILY_ROLLUP -> Option.empty,
    SchedulerAuditType.WEEKLY_ROLLUP -> Option.empty,
    SchedulerAuditType.MONTHLY_ROLLUP -> Option.empty,
    SchedulerAuditType.QUARTERLY_ROLLUP -> Option.empty
  )


  override def receive: Receive = {
    case StatsProcessingFailed(statsProcessorRef, reason, message, schedulerType) =>
      statsProcessorRef ! InitializeAuditor(self)
      errorOutLatestRun(schedulerType, reason, message)

    case StatsProcessingSucceeded(x) =>
      updateSuccess(x)
    case LastRunInfo(x) =>
      getLastRunInfo(x)
    case NextRunInfo(x) =>
      getLatestRunInfo(x)
    case UpdateHiveIds(ids) =>
      updateHiveIdsForLatestRun(ids)
  }

  def getLatestRunInfo(schedulerType: SchedulerAuditType) = {
    val nextEntry: Option[SchedulerRunAudit] = mgr.withTransaction(() => auditService.getNextReadInformation(schedulerType).asScala)
    nextEntry match {
      case None =>
        log.info("Old run has not completed. Retry after 30 seconds")
        sender ! RetryAfter(30 seconds)
      case Some(x) =>
        val runInfo = transformToRunInfo(x)
        currentRunInfoMap += (schedulerType -> Some(runInfo))
        sender ! runInfo
    }

  }

  def getLastRunInfo(schedulerType: SchedulerAuditType) = {
    val lastEntry: Option[SchedulerRunAudit] = mgr.withTransaction(() => auditService.getLastAuditEntry(schedulerType).asScala)
    val nextEntry: Option[SchedulerRunAudit] = mgr.withTransaction(() => auditService.getNextReadInformation(lastEntry.orNull, schedulerType).asScala)
    lastEntry match {
      case None =>
        log.info(s"No run has happened of $schedulerType")
        val newRunInfo = transformToRunInfo(nextEntry.get)
        currentRunInfoMap += (schedulerType -> Some(newRunInfo))
        sender ! EmptyRunInfo(schedulerType)
      case Some(x) =>
        nextEntry match {
          case None =>
            log.info("Old run has not completed. Retry after 30 seconds")
            sender ! RetryAfter(30 seconds)
          case Some(newEntry) =>
            val oldRunInfo = transformToRunInfo(x)
            val newRunInfo = transformToRunInfo(newEntry)
            currentRunInfoMap += (schedulerType -> Some(newRunInfo))
            sender ! oldRunInfo
        }

    }

  }

  private def transformToRunInfo(runAudit: SchedulerRunAudit): RunInfo = {
    if(runAudit.getType != SchedulerAuditType.DAILY_ROLLUP) {
      RunInfo(runAudit.getId, runAudit.getReadStartTime.toLocalDate, Option.empty, runAudit.getType)
    } else {
      if (runAudit.getRetryCount != null && runAudit.getQueriesProcessed != null) {
        val queryIds = runAudit.getQueriesProcessed.split(",").map(_.trim.toLong).toSeq
        if (queryIds.nonEmpty) {
          return RunInfo(runAudit.getId, runAudit.getReadStartTime.toLocalDate, Some(queryIds), runAudit.getType)
        }

      }
      RunInfo(runAudit.getId, runAudit.getReadStartTime.toLocalDate)
    }
  }

  def updateHiveIdsForLatestRun(ids: Seq[Long]): Unit = {
    val idsAsString = ids.map(_.toString).mkString(",")
    log.info("Updating Audit information with hiveIds for latest run")
    currentRunInfoMap(SchedulerAuditType.DAILY_ROLLUP) map { x => mgr.withTransaction(() => auditService.updateHiveIdsProcessed(x.auditId, idsAsString)) }
  }

  private def errorOutLatestRun(schedulerAuditType: SchedulerAuditType, reason: Throwable, message: Option[Any]) = {
    log.error("Updating database for the latest error")
    currentRunInfoMap(schedulerAuditType) map { x => mgr.withTransaction(() => auditService.updateWithError(x.auditId, reason)) }
  }

  private def updateSuccess(schedulerAuditType: SchedulerAuditType) = {
    currentRunInfoMap(schedulerAuditType) map { x => mgr.withTransaction(() => auditService.updateSuccess(x.auditId))}
  }
}
