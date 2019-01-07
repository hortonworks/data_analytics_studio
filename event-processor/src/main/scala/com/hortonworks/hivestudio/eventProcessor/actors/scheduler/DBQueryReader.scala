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

import akka.actor.{Actor, ActorLogging}
import com.google.inject.Inject
import com.hortonworks.hivestudio.eventProcessor.dto.StatsProcessingData
import com.hortonworks.hivestudio.eventProcessor.services.ReportProcessingService
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager

object DBQueryReader {

  case class ReadQueries(ids: Option[Seq[Long]] = None)

  case class QueriesToProcess(explainPlans: Seq[StatsProcessingData])

}

class DBQueryReader @Inject()(mgr: TransactionManager, reportProcessingService: ReportProcessingService) extends Actor with ActorLogging {

  import DBQueryReader._

  import collection.JavaConverters._


  override def receive: Receive = {
    case ReadQueries(x) =>
      sender() ! QueriesToProcess(getNextQueriesToProcess(x))
  }

  def getNextQueriesToProcess(ids: Option[Seq[Long]] = None) = {
    mgr.withTransaction(() => {
      val seq = ids match {
        case None =>
          val queries = reportProcessingService.getNextQueriesToProcess.asScala
          val hiveIds = queries.map( x => x.getHiveId)
          reportProcessingService.updateQueriesAsProcessed(hiveIds.asJava)
          queries
        case Some(x) =>
          val y: Seq[java.lang.Long] = x.map(z => z: java.lang.Long)
          reportProcessingService.getNextQueriesToProcessByHiveQueryIds(y.asJava).asScala
      }
      seq
    })
  }
}
