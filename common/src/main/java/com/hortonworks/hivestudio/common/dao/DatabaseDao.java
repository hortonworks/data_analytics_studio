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

import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.repository.JdbiDao;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@RegisterBeanMapper(Database.class)
public interface DatabaseDao extends JdbiDao<Database, Integer> {

  String NOT_DROPPED_CLAUSE = "(d.dropped is null or d.dropped = false)";

  @SqlQuery("SELECT * FROM das.databases WHERE id = :id")
  Optional<Database> findOne(@Bind("id") Integer id);

  @SqlQuery("SELECT * FROM das.databases d WHERE name = :name AND " + NOT_DROPPED_CLAUSE)
  Optional<Database> findByNameAndNotDropped(@Bind("name") String name);

  @SqlQuery("SELECT * FROM das.databases")
  List<Database> findAll();

  @SqlQuery("SELECT * FROM das.databases d WHERE " + NOT_DROPPED_CLAUSE)
  List<Database> findAllNotDropped();

  @SqlQuery("SELECT * FROM das.databases d WHERE creation_source = :creationSource AND " + NOT_DROPPED_CLAUSE)
  List<Database> findAllByCreationSourceAndNotDropped(@Bind("creationSource") String creationSource);

  @SqlQuery("SELECT * FROM das.databases WHERE name IN (<dbNames>)")
  List<Database> findAllByNames(@BindList("dbNames") Set<String> dbNames);

  @SqlUpdate("INSERT INTO das.databases " +
    "(name, create_time, dropped, dropped_at, last_updated_at, creation_source) " +
    "VALUES " +
    "(:name, :createTime, :dropped, :droppedAt, :lastUpdatedAt, :creationSource)")
  @GetGeneratedKeys
  Integer insert(@BindBean Database entity);

  @SqlUpdate("DELETE FROM das.databases where id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("UPDATE das.databases SET " +
    "name = :name, create_time = :createTime, dropped = :dropped, dropped_at = :droppedAt, " +
    "last_updated_at = :lastUpdatedAt, creation_source = :creationSource where id = :id" )
  int update(@BindBean Database savedQuery);

  @SqlUpdate("INSERT INTO das.databases " +
    "(name, create_time, dropped, dropped_at, last_updated_at, creation_source) " +
    "VALUES " +
    "(:name, :createTime, :dropped, :droppedAt, :lastUpdatedAt, :creationSource) " +
    "ON CONFLICT (name, dropped) WHERE not dropped DO UPDATE SET " +
    "create_time = EXCLUDED.create_time, dropped_at = EXCLUDED.dropped_at, " +
    "last_updated_at = EXCLUDED.last_updated_at, creation_source = EXCLUDED.creation_source")
  @GetGeneratedKeys
  Database upsert(@BindBean Database database);

}
