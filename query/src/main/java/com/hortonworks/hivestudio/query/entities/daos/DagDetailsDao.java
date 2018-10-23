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

import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

@RegisterBeanMapper(DagDetails.class)
public interface DagDetailsDao extends JdbiDao<DagDetails, Long> {
  @Override
  @SqlQuery("select * from das.dag_details where id = :id")
  Optional<DagDetails> findOne(@Bind("id") Long id);

  @Override
  @SqlQuery("select * from das.dag_details")
  Collection<DagDetails> findAll();

  @Override
  @SqlUpdate("insert into das.dag_details (dag_plan, vertex_name_id_mapping, diagnostics, " +
      "counters, hive_query_id, dag_info_id) values (cast(:dagPlan as jsonb), " +
      "cast(:vertexNameIdMapping as jsonb), :diagnostics, cast(:counters as jsonb), " +
      ":hiveQueryId, :dagInfoId)")
  @GetGeneratedKeys
  Long insert(@BindBean DagDetails entity);

  @Override
  @SqlUpdate("delete from das.dag_details where id = :id")
  int delete(@Bind("id") Long id);

  @Override
  @SqlUpdate("update das.dag_details set dag_plan = cast(:dagPlan as jsonb), " +
      "vertex_name_id_mapping = cast(:vertexNameIdMapping as jsonb), " +
      "diagnostics = :diagnostics, counters = cast(:counters as jsonb), " +
      "hive_query_id = :hiveQueryId, dag_info_id = :dagInfoId where id = :id" )
  int update(@BindBean DagDetails QueryDetails);

  @SqlQuery("select dd.* from das.dag_details as dd join das.hive_query as hq " +
      "on dd.hive_query_id = hq.id where hq.query_id = :hiveQueryId")
  Collection<DagDetails> findByHiveQueryId(@Bind("hiveQueryId") String hiveQueryId);

  @SqlQuery("select dd.* from das.dag_details as dd join das.dag_info as di " +
      "on dd.dag_info_id = di.id where di.dag_id = :dagId")
  Optional<DagDetails> findByDagId(@Bind("dagId") String dagId);
}
