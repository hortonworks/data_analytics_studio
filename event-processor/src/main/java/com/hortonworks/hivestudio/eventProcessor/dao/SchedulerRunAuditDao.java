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
package com.hortonworks.hivestudio.eventProcessor.dao;

import com.hortonworks.hivestudio.common.repository.JdbiDao;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerRunAudit;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Collection;
import java.util.Optional;

/**
 * Jdbi repository for Udf
 */
@RegisterBeanMapper(SchedulerRunAudit.class)
public interface SchedulerRunAuditDao extends JdbiDao<SchedulerRunAudit, Integer> {

  @SqlQuery("select * from das.report_scheduler_run_audit where id = :id")
  Optional<SchedulerRunAudit> findOne(@Bind("id") Integer id);

  @SqlQuery("select * from das.report_scheduler_run_audit WHERE type = :type ORDER BY id DESC LIMIT 1")
  Optional<SchedulerRunAudit> findLastAuditEntryByType(@Bind("type") SchedulerAuditType type);

  @SqlQuery("select * from das.report_scheduler_run_audit")
  Collection<SchedulerRunAudit> findAll();

  @SqlUpdate("insert into das.report_scheduler_run_audit (type, read_start_time, read_end_time, queries_processed, status, failure_reason, last_try_id, retry_count) values (:type, :readStartTime, :readEndTime, :queriesProcessed, :status, :failureReason, :lastTryId, :retryCount) ")
  @GetGeneratedKeys
  Integer insert(@BindBean SchedulerRunAudit entity);

  @SqlUpdate("delete from das.report_scheduler_run_audit where id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("update das.report_scheduler_run_audit set type = :type, read_start_time = :readStartTime, read_end_time = :readEndTime, queries_processed = :queriesProcessed, status = :status, failure_reason = :failureReason, last_try_id = :lastTryId, retry_count = :retryCount where id = :id" )
  int update(@BindBean SchedulerRunAudit savedQuery);

}