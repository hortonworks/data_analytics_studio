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
import java.util.List;
import java.util.Optional;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

/**
 * Jdbi dao for QueryDetails
 */
@RegisterBeanMapper(QueryDetails.class)
public interface QueryDetailsDao extends JdbiDao<QueryDetails, Long> {
  @Override
  @SqlQuery("select * from das.query_details where id = :id")
  Optional<QueryDetails> findOne(@Bind("id") Long id);

  @Override
  @SqlQuery("select * from das.query_details")
  Collection<QueryDetails> findAll();

  @Override
  @SqlUpdate("insert into das.query_details (explain_plan, configuration, perf, hive_query_id) " +
      "values (cast(:explainPlan as jsonb), cast(:configuration as jsonb), " +
      "cast(:perf as jsonb), :hiveQueryId)")
  @GetGeneratedKeys
  Long insert(@BindBean QueryDetails entity);

  @Override
  @SqlUpdate("delete from das.query_details where id = :id")
  int delete(@Bind("id") Long id);

  @Override
  @SqlUpdate("update das.query_details set explain_plan = cast(:explainPlan  as jsonb), " +
      "configuration = cast(:configuration  as jsonb), perf = cast(:perf  as jsonb), " +
      "hive_query_id = :hiveQueryId where id = :id" )
  int update(@BindBean QueryDetails QueryDetails);

  @SqlQuery("select qd.* from das.query_details as qd join das.hive_query as hq on qd.hive_query_id = hq.id where hq.query_id = :hiveQueryId")
  Optional<QueryDetails> findByHiveQueryId(@Bind("hiveQueryId") String hiveQueryId);

  @SqlQuery("select qd.* from das.query_details as qd " +
      "join das.hive_query as hq on qd.hive_query_id = hq.id " +
      "join das.dag_info as di on hq.id = di.hive_query_id " +
      "where di.dag_id = :dagId")
  Optional<QueryDetails> findByDagId(@Bind("dagId") String dagId);

  @SqlQuery("SELECT qd.* FROM das.query_details as qd join das.hive_query as hq on qd.hive_query_id = hq.id  WHERE hq.processed = :processedState AND hq.status IN (<statuses>)")
  List<QueryDetails> findNextSetToProcessForStats(@Bind("processedState") boolean processedState, @BindList("statuses") List<String> statuses);

  @SqlQuery("SELECT qd.* FROM das.query_details as qd join das.hive_query as hq on qd.hive_query_id = hq.id  WHERE hq.id IN (<ids>)")
  List<QueryDetails> findHiveQueryDetailsForNextSetOfProcessingByIds(@BindList("ids") List<Long> ids);

}
