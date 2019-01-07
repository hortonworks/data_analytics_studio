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
package com.hortonworks.hivestudio.reporting.entities.repositories;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jdbi.v3.core.statement.Query;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.common.repository.Identifiable;
import com.hortonworks.hivestudio.common.repository.JdbiDao;
import com.hortonworks.hivestudio.common.repository.JdbiRepository;
import com.hortonworks.hivestudio.common.util.TimeHelper;
import com.hortonworks.hivestudio.reporting.ReportGrouping;
import com.hortonworks.hivestudio.reporting.dao.JCSDailyDao;
import com.hortonworks.hivestudio.reporting.dao.JCSMonthlyDao;
import com.hortonworks.hivestudio.reporting.dao.JCSQuarterlyDao;
import com.hortonworks.hivestudio.reporting.dao.JCSWeeklyDao;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnDBResult;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSDaily;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSMonthly;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSQuarterly;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSWeekly;

public class JoinColumnStatRepository {
  @VisibleForTesting
  static final String GET_FOR_DB_FORMAT =
      "select jcs.*, lc.name as leftColumnName, lt.id as leftColumnTableId, " +
      "    lt.name as leftColumnTableName, lt.parsed_table_type as leftColumnTableType, " +
      "    rc.name as rightColumnName, rt.id as rightColumnTableId, " +
      "    rt.name as rightColumnTableName, rt.parsed_table_type as rightColumnTableType " +
      "from das.join_column_stats_%s jcs " +
      "  join das.columns lc on lc.id = jcs.left_column " +
      "  join das.tables lt on lc.table_id = lt.id " +
      "  join das.columns rc on rc.id = jcs.right_column " +
      "  join das.tables rt on rc.table_id = rt.id " +
      "where (rt.db_id = :dbId or lt.db_id = :dbId) " +
      "  and jcs.date between :startDate and :endDate";

  @VisibleForTesting
  static final String GET_FOR_DB_ALG_FORMAT = GET_FOR_DB_FORMAT +
      "  and jcs.algorithm = :algorithm";

  @VisibleForTesting
  static final String GET_FOR_TABLE_FORMAT =
      "select jcs.*, lc.name as leftColumnName, lt.id as leftColumnTableId, " +
      "    lt.name as leftColumnTableName, lt.parsed_table_type as leftColumnTableType, " +
      "    rc.name as rightColumnName, rt.id as rightColumnTableId, " +
      "    rt.name as rightColumnTableName, rt.parsed_table_type as rightColumnTableType " +
      "from das.join_column_stats_%s jcs " +
      "  join das.columns lc on lc.id = jcs.left_column " +
      "  join das.tables lt on lc.table_id = lt.id " +
      "  join das.columns rc on rc.id = jcs.right_column " +
      "  join das.tables rt on rc.table_id = rt.id " +
      "where (lc.table_id = :tableId or rc.table_id = :tableId) " +
      "  and jcs.date between :startDate and :endDate";

  @VisibleForTesting
  static final String UPSERT_QUERY_FORMAT =
      "INSERT INTO das.join_column_stats_%1$s (left_column, right_column, algorithm, " +
      "      inner_join_count, left_outer_join_count, right_outer_join_count, " +
      "      full_outer_join_count, left_semi_join_count, unique_join_count, " +
      "      unknown_join_count, total_join_count, date) " +
      "  (SELECT left_column, right_column, algorithm, SUM(inner_join_count), " +
      "      SUM(left_outer_join_count), SUM(right_outer_join_count), " +
      "      SUM(full_outer_join_count), SUM(left_semi_join_count), SUM(unique_join_count), " +
      "      SUM(unknown_join_count), SUM(total_join_count), :startDate " +
      "   FROM das.join_column_stats_%2$s" +
      "   WHERE date BETWEEN :startDate AND :endDate " +
      "   GROUP BY left_column, right_column, algorithm) " +
      "ON CONFLICT(left_column, right_column, algorithm, date) " +
      "DO UPDATE SET " +
      "      inner_join_count = EXCLUDED.inner_join_count, " +
      "      left_outer_join_count = EXCLUDED.left_outer_join_count, " +
      "      right_outer_join_count = EXCLUDED.right_outer_join_count, " +
      "      full_outer_join_count = EXCLUDED.full_outer_join_count, " +
      "      left_semi_join_count = EXCLUDED.left_semi_join_count, " +
      "      unique_join_count = EXCLUDED.unique_join_count, " +
      "      unknown_join_count = EXCLUDED.unknown_join_count, " +
      "      total_join_count = EXCLUDED.total_join_count";
  private static abstract class RollupStatsRepository<T extends Identifiable<I>, I, D extends JdbiDao<T, I>>
      extends JdbiRepository<T, I, D> implements StatsAggregator {

    private final String upsertQuery;
    private final String dbQuery;
    private final String dbAlgQuery;
    private final String tableQuery;

    protected RollupStatsRepository(D dao, ReportGrouping target, ReportGrouping source) {
      super(dao);
      upsertQuery = String.format(UPSERT_QUERY_FORMAT, target.getTableSuffix(),
          source.getTableSuffix());
      dbQuery = String.format(GET_FOR_DB_FORMAT, target.getTableSuffix());
      dbAlgQuery = String.format(GET_FOR_DB_ALG_FORMAT, target.getTableSuffix());
      tableQuery = String.format(GET_FOR_TABLE_FORMAT, target.getTableSuffix());
    }

    @Override
    public int rollup(LocalDate date) {
      return getDao().withHandle(handle -> handle.createUpdate(upsertQuery)
          .bind("startDate", getStartDate(date))
          .bind("endDate", getEndDate(date))
          .execute());
    }

    protected abstract LocalDate getStartDate(LocalDate date);

    protected abstract LocalDate getEndDate(LocalDate date);

    public List<JoinColumnDBResult> findByDatabaseAndDateRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate, String algorithm) {
      String queryStr = StringUtils.isEmpty(algorithm) ? dbQuery : dbAlgQuery;
      return getDao().withHandle(handle -> {
          Query query = handle.createQuery(queryStr)
            .bind("dbId", databaseId)
            .bind("startDate", startDate)
            .bind("endDate", endDate);
          if (queryStr == dbAlgQuery) {
            query.bind("algorithm", algorithm);
          }
          return query.mapToBean(JCSDaily.class).stream()
              .map(r -> new JoinColumnDBResult(startDate, r))
              .collect(Collectors.toList());
          });
    }

    public List<JoinColumnDBResult> findByTableAndDateRange(Integer tableId, LocalDate startDate, LocalDate endDate) {
      return getDao().withHandle(handle -> handle.createQuery(tableQuery)
        .bind("tableId", tableId)
        .bind("startDate", startDate)
        .bind("endDate", endDate)
        .mapToBean(JCSDaily.class).stream()
        .map(r -> new JoinColumnDBResult(startDate, r))
        .collect(Collectors.toList()));
    }
  }

  public static class Daily extends RollupStatsRepository<JCSDaily, Integer, JCSDailyDao> {
    @Inject
    public Daily(JCSDailyDao dao) {
      super(dao, ReportGrouping.DAILY, ReportGrouping.DAILY);
    }

    @Override
    public Optional<JCSDaily> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<JCSDaily> findAll() {
      return dao.findAll();
    }

    @Override
    public JCSDaily save(JCSDaily entity) {
      if (entity.getId() == null) {
        Integer id = this.getDao().insert(entity);
        entity.setId(id);
      } else {
        this.getDao().update(entity);
      }

      return entity;
    }

    @Override
    public boolean delete(Integer id) {
      return dao.delete(id) != 0;
    }

    public List<JoinColumnDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new JoinColumnDBResult(startDate, t)).collect(Collectors.toList());
    }

    @Override
    public int rollup(LocalDate date) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected LocalDate getStartDate(LocalDate date) {
      return null;
    }

    @Override
    protected LocalDate getEndDate(LocalDate date) {
      return null;
    }
  }

  public static class Weekly extends RollupStatsRepository<JCSWeekly, Integer, JCSWeeklyDao> {
    @Inject
    public Weekly(JCSWeeklyDao dao) {
      super(dao, ReportGrouping.WEEKLY, ReportGrouping.DAILY);
    }

    @Override
    public Optional<JCSWeekly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<JCSWeekly> findAll() {
      return dao.findAll();
    }

    @Override
    public JCSWeekly save(JCSWeekly entity) {
      if (entity.getId() == null) {
        Integer id = this.getDao().insert(entity);
        entity.setId(id);
      } else {
        this.getDao().update(entity);
      }

      return entity;
    }

    @Override
    public boolean delete(Integer id) {
      return dao.delete(id) != 0;
    }

    public List<JoinColumnDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new JoinColumnDBResult(startDate, t)).collect(Collectors.toList());
    }

    @Override
    protected LocalDate getStartDate(LocalDate date) {
      return TimeHelper.getWeekStartDate(date);
    }

    @Override
    protected LocalDate getEndDate(LocalDate date) {
      return TimeHelper.getWeekEndDate(date);
    }
  }

  public static class Monthly extends RollupStatsRepository<JCSMonthly, Integer, JCSMonthlyDao> {

    @Inject
    public Monthly(JCSMonthlyDao dao) {
      super(dao, ReportGrouping.MONTHLY, ReportGrouping.DAILY);
    }

    @Override
    public Optional<JCSMonthly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<JCSMonthly> findAll() {
      return dao.findAll();
    }

    @Override
    public JCSMonthly save(JCSMonthly entity) {
      if (entity.getId() == null) {
        Integer id = this.getDao().insert(entity);
        entity.setId(id);
      } else {
        this.getDao().update(entity);
      }

      return entity;
    }

    @Override
    public boolean delete(Integer id) {
      return dao.delete(id) != 0;
    }

    public List<JoinColumnDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new JoinColumnDBResult(startDate, t)).collect(Collectors.toList());
    }

    @Override
    protected LocalDate getStartDate(LocalDate date) {
      return TimeHelper.getMonthStartDate(date);
    }

    @Override
    protected LocalDate getEndDate(LocalDate date) {
      return TimeHelper.getMonthEndDate(date);
    }
  }

  public static class Quarterly
      extends RollupStatsRepository<JCSQuarterly, Integer, JCSQuarterlyDao> {
    @Inject
    public Quarterly(JCSQuarterlyDao dao) {
      super(dao, ReportGrouping.QUARTERLY, ReportGrouping.MONTHLY);
    }

    @Override
    public Optional<JCSQuarterly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<JCSQuarterly> findAll() {
      return dao.findAll();
    }

    @Override
    public JCSQuarterly save(JCSQuarterly entity) {
      if (entity.getId() == null) {
        Integer id = this.getDao().insert(entity);
        entity.setId(id);
      } else {
        this.getDao().update(entity);
      }

      return entity;
    }

    @Override
    public boolean delete(Integer id) {
      return dao.delete(id) != 0;
    }

    public List<JoinColumnDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new JoinColumnDBResult(startDate, t)).collect(Collectors.toList());
    }

    @Override
    protected LocalDate getStartDate(LocalDate date) {
      return TimeHelper.getQuarterStartDate(date);
    }

    @Override
    protected LocalDate getEndDate(LocalDate date) {
      return TimeHelper.getQuarterEndDate(date);
    }
  }
}
