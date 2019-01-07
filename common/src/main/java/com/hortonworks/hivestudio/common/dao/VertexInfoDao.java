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
package com.hortonworks.hivestudio.common.dao;

import java.util.Collection;
import java.util.Optional;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.hortonworks.hivestudio.common.entities.VertexInfo;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

@RegisterBeanMapper(VertexInfo.class)
public interface VertexInfoDao extends JdbiDao<VertexInfo, Long> {

  @Override
  @SqlQuery("select * from das.vertex_info where id = :id")
  Optional<VertexInfo> findOne(@Bind("id") Long id);

  @SqlQuery("select * from das.vertex_info where vertex_id = :vertexId LIMIT 1")
  Optional<VertexInfo> findByVertexId(@Bind("vertexId") String vertexId);

  @Override
  @SqlQuery("select * from das.vertex_info")
  Collection<VertexInfo> findAll();

  @SqlQuery("select vi.* from das.vertex_info vi JOIN das.dag_info di on vi.dag_id = di.id where di.dag_id = :dagId")
  Collection<VertexInfo> findAllByDagId(@Bind("dagId") String dagId);

  @Override
  @SqlUpdate("insert into das.vertex_info" +
    "(name, vertex_id, dag_id, domain_id, task_count, succeeded_task_count, completed_task_count," +
    "failed_task_count, killed_task_count, failed_task_attempt_count, killed_task_attempt_count," +
    "class_name, start_time, end_time, init_requested_time, start_requested_time, status," +
    "counters, stats, events)" +
    "values" +
    "(:name, :vertexId, :dagId, :domainId, :taskCount, :succeededTaskCount, :completedTaskCount," +
    ":failedTaskCount, :killedTaskCount, :failedTaskAttemptCount, :killedTaskAttemptCount," +
    ":className, :startTime, :endTime, :initRequestedTime, :startRequestedTime, :status," +
    "cast(:counters as jsonb), cast(:stats as jsonb), cast(:events as jsonb))")
  @GetGeneratedKeys
  Long insert(@BindBean VertexInfo entity);

  @Override
  @SqlUpdate("delete from das.vertex_info where id = :id")
  int delete(@Bind("id") Long id);

  @Override
  @SqlUpdate("update das.vertex_info set " +
    "name = :name, vertex_id = :vertexId, dag_id = :dagId, domain_id = :domainId, task_count = :taskCount," +
    "succeeded_task_count = :succeededTaskCount, completed_task_count = :completedTaskCount," +
    "failed_task_count = :failedTaskCount, killed_task_count = :killedTaskCount," +
    "failed_task_attempt_count = :failedTaskAttemptCount, killed_task_attempt_count = :killedTaskAttemptCount," +
    "class_name = :className, start_time = :startTime, end_time = :endTime," +
    "init_requested_time = :initRequestedTime, start_requested_time = :startRequestedTime, status = :status," +
    "counters = cast(:counters as jsonb), stats = cast(:stats as jsonb), events = cast(:events as jsonb) " +
    "where id = :id" )
  int update(@BindBean VertexInfo savedQuery);

}