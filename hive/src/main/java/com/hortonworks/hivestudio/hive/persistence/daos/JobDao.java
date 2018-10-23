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
package com.hortonworks.hivestudio.hive.persistence.daos;

import com.hortonworks.hivestudio.common.repository.JdbiDao;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Collection;
import java.util.Optional;

@RegisterBeanMapper(Job.class)
public interface JobDao extends JdbiDao<Job, Integer> {
  /**
   * Finds the record entity with the given id
   * @param id identity of the record
   * @return return Optional of entity if found Optional.empty if not found
   */
  @SqlQuery("select * from das.jobs where id = :id")
  Optional<Job> findOne(@Bind("id") Integer id);

  /**
   * Finds the record entity without TEXT fields with the given id. Can be used for internal uses
   * @param id identity of the record
   * @return return Optional of entity if found Optional.empty if not found
   */
  @SqlQuery("select owner, title, status_dir, date_submitted, duration, selected_database, " +
      "status, referrer, logFile, guid from das.jobs where id = :id")
  Optional<Job> findOneLite(@Bind("id") Integer id);

//  TODO : need another api to fetch as an iterator for really big results.
  /**
   * Returns all the records for this entity type.
   * Use with caution as the returned collection can be big.
   * This all is loaded from db to memory.
   * @return Collection of all records
   */
  @SqlQuery("select * from das.jobs")
  Collection<Job> findAll();

  /**
   * Returns all the records for this entity type.
   * Use with caution as the returned collection can be big.
   * This all is loaded from db to memory.
   * @return Collection of all records
   */
  @SqlQuery("select owner, title, status_dir, date_submitted, duration, selected_database, " +
      "status, referrer, logFile, guid from das.jobs from das.jobs")
  Collection<Job> findAllLite();

  /**
   * inserts the entity
   * @param job Job object to be inserted.
   * @return The Integer identity generated
   */
  @SqlUpdate("insert into das.jobs (owner, title, status_dir, date_submitted, duration, query, selected_database," +
      " status, referrer, global_settings, log_file, guid ) " +
      " values (:owner, :title, :statusDir, :dateSubmitted, :duration, :query, :selectedDatabase, " +
      " :status, :referrer, :globalSettings, :logFile, :guid )")
  @GetGeneratedKeys
  Integer insert(@BindBean Job job);

  /**
   * updates the entity
   * @param entity Job object to be updated.
   * @return The updated count. Should be 1 if the entity exists with given id
   */
  @SqlUpdate("update das.jobs  set owner = :owner, title = :title, status_dir = :statusDir, " +
      "date_submitted = :dateSubmitted, duration = :duration, query = :query, selected_database = :selectedDatabase, " +
      "status = :status, referrer = :referrer, global_settings = :globalSettings, log_file = :logFile, guid = :guid " +
      "where id = :id" )
  int update(@BindBean Job entity);

  /**
   * Deletes the entities
   * @return number of the deleted record. should be 1 if the entity exists else 0
   */
  @SqlUpdate("delete from das.jobs where id = :id")
  int delete(Integer id);
}
