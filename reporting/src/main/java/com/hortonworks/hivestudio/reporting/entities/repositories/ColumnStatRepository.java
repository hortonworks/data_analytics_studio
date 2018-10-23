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

import com.google.inject.Inject;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.repository.Identifiable;
import com.hortonworks.hivestudio.common.repository.JdbiDao;
import com.hortonworks.hivestudio.common.repository.JdbiRepository;
import com.hortonworks.hivestudio.common.util.TimeHelper;
import com.hortonworks.hivestudio.reporting.ReportGrouping;
import com.hortonworks.hivestudio.reporting.dao.CSDailyDao;
import com.hortonworks.hivestudio.reporting.dao.CSMonthlyDao;
import com.hortonworks.hivestudio.reporting.dao.CSQuarterlyDao;
import com.hortonworks.hivestudio.reporting.dao.CSWeeklyDao;
import com.hortonworks.hivestudio.reporting.dto.count.ColumnStatsResult;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSDaily;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSMonthly;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSQuarterly;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSWeekly;

import lombok.Getter;

public class ColumnStatRepository {
  private static final String SCHEMA = Constants.DATABASE_SCHEMA;

  @Getter
  public static class ColumnStatsDBResult {
    private final Integer tableId;
    private final Integer columnId;
    private final LocalDate date;
    private final ColumnStatsResult stat;

    private Optional<CSDaily> csDailyOptional = Optional.empty();
    private Optional<CSWeekly> csWeeklyOptional = Optional.empty();
    private Optional<CSMonthly> csMonthlyOptional = Optional.empty();
    private Optional<CSQuarterly> csQuarterlyOptional = Optional.empty();

    private ColumnStatsDBResult(Integer tableId, Integer columnId, LocalDate date,
        ColumnStatsResult stat) {
      this.tableId = tableId;
      this.columnId = columnId;
      this.date = date;
      this.stat = stat;
    }

    public ColumnStatsDBResult(CSDaily entity) {
      this(entity.getTableId(), entity.getColumnId(), entity.getDate(),
          new ColumnStatsResult(entity));
      this.csDailyOptional = Optional.ofNullable(entity);
    }

    public ColumnStatsDBResult(CSWeekly entity) {
      this(entity.getTableId(), entity.getColumnId(), entity.getDate(),
          new ColumnStatsResult(entity));
      this.csWeeklyOptional = Optional.ofNullable(entity);
    }

    public ColumnStatsDBResult(CSMonthly entity) {
      this(entity.getTableId(), entity.getColumnId(), entity.getDate(),
          new ColumnStatsResult(entity));
      this.csMonthlyOptional = Optional.ofNullable(entity);
    }

    public ColumnStatsDBResult(CSQuarterly entity) {
      this(entity.getTableId(), entity.getColumnId(), entity.getDate(),
          new ColumnStatsResult(entity));
      this.csQuarterlyOptional = Optional.ofNullable(entity);
    }
  }

  private static abstract class RollupStatsRepository<T extends Identifiable<I>, I, D extends JdbiDao<T, I>>
      extends JdbiRepository<T, I, D> implements StatsAggregator {
    private static String UPSERT_QUERY_FORMAT =
        "INSERT INTO %1$s.column_stats_%2$s (column_id, join_count, filter_count," +
        "      aggregation_count, projection_count, date) " +
        "  (SELECT column_id, SUM(join_count), SUM(filter_count), SUM(aggregation_count), " +
        "      SUM(projection_count), :startDate FROM %1$s.column_stats_%3$s" +
        "   WHERE date BETWEEN :startDate AND :endDate GROUP BY column_id) " +
        "ON CONFLICT(column_id, date) " + "DO UPDATE SET " +
        "      join_count = EXCLUDED.join_count, " +
        "      filter_count = EXCLUDED.filter_count, " +
        "      aggregation_count = EXCLUDED.aggregation_count, " +
        "      projection_count = EXCLUDED.projection_count";

    private final String upsertQuery;

    protected RollupStatsRepository(D dao, ReportGrouping target, ReportGrouping source) {
      super(dao);
      upsertQuery = String.format(UPSERT_QUERY_FORMAT, SCHEMA, target.getTableSuffix(),
          source.getTableSuffix());
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
  }

  public static class Daily extends JdbiRepository<CSDaily, Integer, CSDailyDao> {
    @Inject
    public Daily(CSDailyDao dao) {
      super(dao);
    }

    @Override
    public Optional<CSDaily> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<CSDaily> findAll() {
      return dao.findAll();
    }

    @Override
    public CSDaily save(CSDaily entity) {
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

    public List<ColumnStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTablesWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }
  }

  public static class Weekly extends RollupStatsRepository<CSWeekly, Integer, CSWeeklyDao> {
    @Inject
    public Weekly(CSWeeklyDao dao) {
      super(dao, ReportGrouping.WEEKLY, ReportGrouping.DAILY);
    }

    @Override
    public Optional<CSWeekly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<CSWeekly> findAll() {
      return dao.findAll();
    }

    @Override
    public CSWeekly save(CSWeekly entity) {
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

    public List<ColumnStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTablesWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
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

  public static class Monthly extends RollupStatsRepository<CSMonthly, Integer, CSMonthlyDao> {
    @Inject
    public Monthly(CSMonthlyDao dao) {
      super(dao, ReportGrouping.MONTHLY, ReportGrouping.DAILY);
    }

    @Override
    public Optional<CSMonthly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<CSMonthly> findAll() {
      return dao.findAll();
    }

    @Override
    public CSMonthly save(CSMonthly entity) {
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

    public List<ColumnStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTablesWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
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
      extends RollupStatsRepository<CSQuarterly, Integer, CSQuarterlyDao> {
    @Inject
    public Quarterly(CSQuarterlyDao dao) {
      super(dao, ReportGrouping.QUARTERLY, ReportGrouping.MONTHLY);
    }

    @Override
    public Optional<CSQuarterly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<CSQuarterly> findAll() {
      return dao.findAll();
    }

    @Override
    public CSQuarterly save(CSQuarterly entity) {
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

    public List<ColumnStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTablesWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<ColumnStatsDBResult> findByColumnsAndTimeRange(List<Integer> columnIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForColumnsWithinTimeRange(columnIds, startDate, endDate).stream()
          .map(t -> new ColumnStatsDBResult(t)).collect(Collectors.toList());
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
