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
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSDaily;

@RegisterBeanMapper(CSDaily.class)
public interface CSDailyDao extends JdbiDao<CSDaily, Integer> {
  @SqlQuery("select * from das.column_stats_daily where id = :id")
  Optional<CSDaily> findOne(@Bind("id") Integer id);

  @SqlQuery("select * from das.column_stats_daily")
  Collection<CSDaily> findAll();

  @SqlUpdate("insert into das.column_stats_daily (column_id, join_count, filter_count, " +
      "aggregation_count, projection_count, date) values (:columnId, :joinCount, :filterCount, " +
      ":aggregationCount, :projectionCount, :date)")
  @GetGeneratedKeys
  Integer insert(@BindBean CSDaily entity);

  @SqlUpdate("update das.column_stats_daily set column_id = :columnId, join_count = :joinCount, " +
      "filter_count = :filterCount, aggregation_count = :aggregationCount, " +
      "projection_count = :projectionCount, date = :date where id = :id" )
  int update(@BindBean CSDaily entity);

  @SqlUpdate("delete from das.column_stats_daily where id = :id")
  int delete(@Bind("id") Integer id);

  @SqlQuery("select cs.*, c.table_id from das.column_stats_daily cs " +
      "join das.columns c on cs.column_id = c.id join das.tables t on c.table_id = t.id " +
      "where t.db_id = :dbId AND cs.date between :startDate and :endDate")
  Collection<CSDaily> getAllForDatabaseWithinTimeRange(@Bind("dbId") Integer dbId,
      @Bind("startDate") LocalDate startDate, @Bind("endDate") LocalDate endDate);

  @SqlQuery("select cs.*, c.table_id from das.column_stats_daily cs " +
      "join das.columns c on cs.column_id = c.id " +
      "where c.table_id in (<tableIds>) and cs.date between :startDate and :endDate")
  Collection<CSDaily> getAllForTablesWithinTimeRange(@BindList("tableIds") List<Integer> tableIds,
      @Bind("startDate") LocalDate startDate, @Bind("endDate") LocalDate endDate);

  @SqlQuery("select cs.*, c.table_id from das.column_stats_daily cs " +
      "join das.columns c on cs.column_id = c.id " +
      "where cs.column_id in (<columnIds>) and cs.date between :startDate and :endDate")
  Collection<CSDaily> getAllForColumnsWithinTimeRange(@BindList("columnIds") List<Integer> columnIds,
      @Bind("startDate") LocalDate startDate, @Bind("endDate") LocalDate endDate);
}
