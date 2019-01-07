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

package com.hortonworks.hivestudio.reporting.dao;

import java.time.LocalDate;
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

import com.hortonworks.hivestudio.common.repository.JdbiDao;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSDaily;

@RegisterBeanMapper(JCSDaily.class)
public interface JCSDailyDao extends JdbiDao<JCSDaily, Integer> {
  @SqlQuery("select * from das.join_column_stats_daily where id = :id")
  Optional<JCSDaily> findOne(@Bind("id") Integer id);

  @SqlQuery("select * from das.join_column_stats_daily")
  Collection<JCSDaily> findAll();

  @SqlUpdate("insert into das.join_column_stats_daily (inner_join_count, left_outer_join_count, " +
      "right_outer_join_count, full_outer_join_count, left_semi_join_count, unique_join_count, " +
      "unknown_join_count, total_join_count, date, algorithm, left_column, right_column) values " +
      "(:innerJoinCount, :leftOuterJoinCount, :rightOuterJoinCount, :fullOuterJoinCount, " +
      ":leftSemiJoinCount, :uniqueJoinCount, :unknownJoinCount, :totalJoinCount, :date, " +
      ":algorithm, :leftColumn, :rightColumn)")
  @GetGeneratedKeys
  Integer insert(@BindBean JCSDaily entity);

  @SqlUpdate("update das.join_column_stats_daily set inner_join_count = :innerJoinCount, " +
      "left_outer_join_count = :leftOuterJoinCount, right_outer_join_count = :rightOuterJoinCount, " +
      "full_outer_join_count = :fullOuterJoinCount, left_semi_join_count = :leftSemiJoinCount, " +
      "unique_join_count = :uniqueJoinCount, unknown_join_count = :unknownJoinCount, " +
      "total_join_count = :totalJoinCount, date = :date, algorithm = :algorithm, " +
      "left_column = :leftColumn, right_column = :rightColumn where id = :id" )
  int update(@BindBean JCSDaily entity);

  @SqlUpdate("delete from das.join_column_stats_daily where id = :id")
  int delete(@Bind("id") Integer id);
  @SqlQuery("select jcs.*, lc.name as leftColumnName, lt.id as leftColumnTableId, " +
      "lt.name as leftColumnTableName, rc.name as rightColumnName, rt.id as rightColumnTableId, " +
      "rt.name as rightColumnTableName from das.join_column_stats_daily jcs " +
      "join das.columns lc on lc.id = jcs.left_column join das.tables lt on lc.table_id = lt.id " +
      "join das.columns rc on rc.id = jcs.right_column join das.tables rt on rc.table_id = rt.id " +
      "where jcs.left_column in (<columnIds>) and jcs.right_column in (<columnIds>) " +
      "and jcs.date between :startDate and :endDate")
  Collection<JCSDaily> getAllForColumnsWithinTimeRange(@BindList("columnIds") List<Integer> columnIds,
      @Bind("startDate") LocalDate startDate, @Bind("endDate") LocalDate endDate);
}
