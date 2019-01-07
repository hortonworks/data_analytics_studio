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
package com.hortonworks.hivestudio.query.entities.daos;

import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.repository.JdbiDao;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Jdbi dao for HiveQuery
 */
@RegisterBeanMapper(HiveQuery.class)
public interface HiveQueryDao extends JdbiDao<HiveQuery, Long> {
  @Override
  @SqlQuery("select * from das.hive_query where id = :id")
  Optional<HiveQuery> findOne(@Bind("id") Long id);

  @Override
  @SqlQuery("select * from das.hive_query")
  Collection<HiveQuery> findAll();

  @Override
  @SqlUpdate("insert into das.hive_query (query_id,query,start_time,end_time,elapsed_time,status," +
      "queue_name,user_id,request_user,cpu_time,physical_memory,virtual_memory,data_read,data_written,operation_id,client_ip_address," +
      "hive_instance_address,hive_instance_type,session_id,log_id,thread_id,execution_mode,tables_read,tables_written, databases_used, domain_id," +
      "llap_app_id,used_cbo,processed) " +
      "values" +
      " (:queryId,:query,:startTime,:endTime,:elapsedTime,:status," +
      ":queueName,:userId,:requestUser,:cpuTime,:physicalMemory,:virtualMemory,:dataRead,:dataWritten,:operationId,:clientIpAddress," +
      ":hiveInstanceAddress,:hiveInstanceType,:sessionId,:logId,:threadId,:executionMode,cast(:tablesRead as jsonb),cast( :tablesWritten as jsonb),cast( :databasesUsed as jsonb),:domainId," +
      ":llapAppId,:usedCBO,:processed)")
  @GetGeneratedKeys
  Long insert(@BindBean HiveQuery entity);

  @Override
  @SqlUpdate("delete from das.hive_query where id = :id")
  int delete(@Bind("id") Long id);

  @Override
  @SqlUpdate("update das.hive_query set query_id = :queryId, query = :query, start_time = :startTime, end_time = :endTime," +
      " elapsed_time = :elapsedTime, status = :status, queue_name = :queueName, user_id = :userId, request_user = :requestUser," +
      " cpu_time = :cpuTime, physical_memory = :physicalMemory, virtual_memory = :virtualMemory, data_read = :dataRead, " +
      "data_written = :dataWritten, operation_id = :operationId, client_ip_address = :clientIpAddress, " +
      "hive_instance_address = :hiveInstanceAddress, hive_instance_type = :hiveInstanceType, session_id = :sessionId, " +
      "log_id = :logId, thread_id = :threadId, execution_mode = :executionMode, tables_read = cast(:tablesRead as jsonb), " +
      "tables_written = cast( :tablesWritten as jsonb), databases_used = cast( :databasesUsed as jsonb), domain_id = :domainId, llap_app_id = :llapAppId, used_cbo = :usedCBO, processed = :processed" +
      " where id = :id" )
  int update(@BindBean HiveQuery HiveQuery);

  @SqlUpdate("update das.hive_query SET processed = true WHERE id in (<hiveIds>)")
  int updateProcessed(@BindList("hiveIds") List<Long> hiveIds);

  @SqlUpdate("UPDATE das.hive_query SET end_time = :endTime, cpu_time = :cpuTime, " +
      "physical_memory = :physicalMemory, virtual_memory = :virtualMemory, " +
      "data_read = :dataRead, data_written = :dataWritten WHERE id = :hiveId ")
  int updateStats(@Bind("hiveId") Long id, @Bind("endTime") Long endTime, @Bind("cpuTime") Long cpuTime,
                  @Bind("physicalMemory") Long physicalMemory, @Bind("virtualMemory") Long virtualMemory, @Bind("dataRead") Long dataRead,
                  @Bind("dataWritten") Long dataWritten);

  @SqlQuery("select * from das.hive_query where query_id = :queryId")
  Optional<HiveQuery> findByHiveQueryId(@Bind("queryId") String queryId);
}
