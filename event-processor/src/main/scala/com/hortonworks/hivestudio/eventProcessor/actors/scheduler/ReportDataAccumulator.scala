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
import akka.actor.{Actor, ActorLogging, AllForOneStrategy}
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension
import com.hortonworks.hivestudio.eventProcessor.dto.{ParsedPlan, StatsProcessingData}
import com.hortonworks.hivestudio.hivetools.parsers.QueryPlanParser

object ReportDataAccumulator {

  case class ProcessQueries(explainPlans: Seq[StatsProcessingData])

  case object ProcessingCompleted

  case object FinalizationCompleted
  case class ProcessQueryData(queryData: ParsedPlan)

  case class ProcessingSucceeded(hiveIds: Seq[Long])

}

class ReportDataAccumulator @Inject()(extensionProvider: Provider[GuiceAkkaExtension.AkkaExtensionProvider],
                                      queryParser: QueryPlanParser) extends Actor with ActorLogging {

  import ReportDataAccumulator._

  private val extension = extensionProvider.get()

  private var waitCount = 0
  private var hiveIdsCurrentlyProcessing = Seq[Long]()
  private var currentPlans: Seq[ParsedPlan] = _

  override def receive: Receive = {
    case ProcessQueries(plans) =>
      currentPlans = plans.flatMap(x => {
        try {
          Some(new ParsedPlan(queryParser.parse(x.getPlan, x.getTablesWritten),
              x.getCounters, x.getDate))
        } catch {
          case e: Throwable =>
            log.error(s"Failed to parse the plan of query with id : ${x.getHiveId}. Exception: ${e.getMessage}")
            None
        }

      })
      hiveIdsCurrentlyProcessing = plans.map(_.getHiveId.asInstanceOf[Long])
      processArtifactUpdates()
      context.become(waitForArtifactsToUpdate);
  }

  def waitForArtifactsToUpdate: Receive = {
    case FinalizationCompleted =>
      log.info("Required hive artifacts are updated into database. Updating the counts.")
      processPlans()
      context.become(waitForDBUpdateToComplete)
  }

  def waitForDBUpdateToComplete: Receive = {
    case FinalizationCompleted =>
      waitCount -= 1
      if (waitCount <= 0) {
        waitCount = 0
        finalizationComplete
        context.become(receive)
      }

  }

  def processArtifactUpdates() = {
    val dBArtifactUpdater = context.actorOf(extension.props(classOf[DBArtifactUpdater]), "DBArtifactUpdated")
    log.info("Updating the required hive artifacts into database")
    currentPlans.foreach(x => {
      dBArtifactUpdater ! ProcessQueryData(x)
    })

    dBArtifactUpdater ! ProcessingCompleted
  }

  def processPlans(): Unit = {
    val tableAccumulator = context.actorOf(extension.props(classOf[TableAccumulator]), "TableAccumulator")
    val columnAccumulator = context.actorOf(extension.props(classOf[ColumnAccumulator]), "ColumnAccumulator")
    val joinAccumulator = context.actorOf(extension.props(classOf[JoinAccumulator]), "JoinAccumulator")

    currentPlans.foreach(x => {
      tableAccumulator ! ProcessQueryData(x)
      columnAccumulator ! ProcessQueryData(x)
      joinAccumulator ! ProcessQueryData(x)
    })

    tableAccumulator ! ProcessingCompleted
    columnAccumulator ! ProcessingCompleted
    joinAccumulator ! ProcessingCompleted
    waitCount = 3
  }

  def finalizationComplete: Unit = {
    log.info("Finalization completed for all count processors. Finishing this run.")
    context.parent ! ProcessingSucceeded(hiveIdsCurrentlyProcessing)
  }

  override def supervisorStrategy =
    AllForOneStrategy() {
      case _: Exception => Escalate
    }
}
