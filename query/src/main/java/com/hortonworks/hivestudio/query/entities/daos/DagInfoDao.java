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

import java.util.Collection;
import java.util.Optional;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

@RegisterBeanMapper(DagInfo.class)
public interface DagInfoDao extends JdbiDao<DagInfo, Long> {
  @Override
  @SqlQuery("select * from das.dag_info where id = :id")
  Optional<DagInfo> findOne(@Bind("id") Long id);

  @Override
  @SqlQuery("select * from das.dag_info")
  Collection<DagInfo> findAll();

  @Override
  @SqlUpdate("insert into das.dag_info (dag_id, dag_name, application_id, init_time, start_time," +
      " end_time, status, am_webservice_ver, am_log_url, queue_name, caller_id, caller_type," +
      " hive_query_id, created_at, source_file) values (:dagId, :dagName, :applicationId, :initTime, " +
      ":startTime, :endTime, :status, :amWebserviceVer, :amLogUrl, :queueName, :callerId, " +
      ":callerType, :hiveQueryId, :createdAt, :sourceFile)")
  @GetGeneratedKeys
  Long insert(@BindBean DagInfo entity);

  @Override
  @SqlUpdate("delete from das.dag_info where id = :id")
  int delete(@Bind("id") Long id);

  @Override
  @SqlUpdate("update das.dag_info set dag_id = :dagId, dag_name = :dagName, application_id = :applicationId," +
      "init_time = :initTime, start_time = :startTime, end_time = :endTime, status = :status," +
      "am_webservice_ver = :amWebserviceVer, am_log_url = :amLogUrl, queue_name = :queueName," +
      "caller_id = :callerId, caller_type = :callerType, hive_query_id = :hiveQueryId, " +
      "created_at = :createdAt, source_file = :sourceFile where id = :id")
  int update(@BindBean DagInfo entity);

  @SqlQuery("select * from das.dag_info where hive_query_id = :id")
  Optional<DagInfo> getByHiveQueryTableId(@Bind("id") Long id);

  @SqlQuery("select * from das.dag_info where dag_id = :dagId")
  Optional<DagInfo> getByDagId(@Bind("dagId") String dagId);
}
