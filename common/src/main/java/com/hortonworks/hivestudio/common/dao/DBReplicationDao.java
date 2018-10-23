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

import com.hortonworks.hivestudio.common.entities.DBReplicationEntity;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

@RegisterBeanMapper(DBReplicationEntity.class)
public interface DBReplicationDao extends JdbiDao<DBReplicationEntity, Integer> {
  @SqlQuery("select * from das.db_replication_info where id = :id")
  Optional<DBReplicationEntity> findOne(@Bind("id") Integer id);

  @SqlQuery("select * from das.db_replication_info")
  Collection<DBReplicationEntity> findAll();

  @SqlUpdate("insert into das.db_replication_info (database_name, last_replication_id, " +
      "last_replication_start_time, last_replication_end_time, next_replication_start_time) " +
      "values (:databaseName, :lastReplicationId, :lastReplicationStartTime, " +
      ":lastReplicationEndTime, :nextReplicationStartTime)")
  @GetGeneratedKeys
  Integer insert(@BindBean DBReplicationEntity entity);

  @SqlUpdate("delete from das.db_replication_info where id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("update das.db_replication_info set database_name = :databaseName, " +
      "last_replication_id = :lastReplicationId, " +
      "last_replication_start_time = :lastReplicationStartTime, " +
      "last_replication_end_time = :lastReplicationEndTime, " +
      "next_replication_start_time = :nextReplicationStartTime where id = :id" )
  int update(@BindBean DBReplicationEntity savedQuery);

  @SqlQuery("select * from das.db_replication_info where database_name = :dbName")
  Optional<DBReplicationEntity> findByDatabaseName(@Bind("dbName") String dbName);
}
