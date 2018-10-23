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
import com.hortonworks.hivestudio.reporting.dao.TSDailyDao;
import com.hortonworks.hivestudio.reporting.dao.TSMonthlyDao;
import com.hortonworks.hivestudio.reporting.dao.TSQuarterlyDao;
import com.hortonworks.hivestudio.reporting.dao.TSWeeklyDao;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TSDaily;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TSMonthly;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TSQuarterly;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TSWeekly;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TableStats;

import lombok.AllArgsConstructor;
import lombok.Data;

public class TableStatRepository {
  private static final String SCHEMA = Constants.DATABASE_SCHEMA;

  @Data
  @AllArgsConstructor
  public static class TableStatsDBResult {
    private Integer tableId;
    private LocalDate date;
    private Integer readCount;
    private Integer writeCount;
    private Long bytesRead;
    private Long recordsRead;
    private Long bytesWritten;
    private Long recordsWritten;

    private Optional<TSDaily> tableStatsDaily = Optional.empty();

    private TableStatsDBResult(TableStats stats) {
      this.tableId = stats.getId();
      setCounts(stats);
    }

    private void setCounts(TableStats stats) {
      this.date = stats.getDate();
      this.readCount = stats.getReadCount();
      this.writeCount = stats.getWriteCount();
      this.bytesRead = stats.getBytesRead();
      this.recordsRead = stats.getRecordsRead();
      this.bytesWritten = stats.getBytesWritten();
      this.recordsWritten = stats.getRecordsWritten();
    }

    public TableStatsDBResult(TSDaily stats) {
      this.tableId = stats.getTableId();
      setCounts(stats);
      this.tableStatsDaily = Optional.ofNullable(stats);
    }
  }

  private static abstract class RollupStatsRepository<T extends Identifiable<I>, I, D extends JdbiDao<T, I>>
      extends JdbiRepository<T, I, D> implements StatsAggregator {
    private static String UPSERT_QUERY_FORMAT =
        "INSERT INTO %1$s.table_stats_%2$s (table_id, read_count, write_count, bytes_read, " +
        "    records_read, bytes_written, records_written, date) " +
        "  (SELECT table_id, SUM(read_count), SUM(write_count), SUM(bytes_read), " +
        "      SUM(records_read), SUM(bytes_written), SUM(records_written), :startDate " +
        "   FROM %1$s.table_stats_%3$s" +
        "   WHERE date BETWEEN :startDate AND :endDate GROUP BY table_id) " +
        "ON CONFLICT(table_id, date) " +
        "DO UPDATE SET " +
        "      read_count = EXCLUDED.read_count, " +
        "      write_count = EXCLUDED.write_count, " +
        "      bytes_read = EXCLUDED.bytes_read, " +
        "      records_read = EXCLUDED.records_read, " +
        "      bytes_written = EXCLUDED.bytes_written, " +
        "      records_written = EXCLUDED.records_written";

    private final String upsertQuery;

    protected RollupStatsRepository(D dao, ReportGrouping target, ReportGrouping source){
      super(dao);
      upsertQuery = String.format(UPSERT_QUERY_FORMAT, SCHEMA,
          target.getTableSuffix(), source.getTableSuffix());
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

  public static class Daily extends JdbiRepository<TSDaily, Integer, TSDailyDao> {
    @Inject
    public Daily(TSDailyDao dao){
      super(dao);
    }

    @Override
    public Optional<TSDaily> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<TSDaily> findAll() {
      return dao.findAll();
    }

    @Override
    public TSDaily save(TSDaily entity) {
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

    public List<TableStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<TableStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTableWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
    }
  }

  public static class Weekly extends RollupStatsRepository<TSWeekly, Integer, TSWeeklyDao> {
    @Inject
    public Weekly(TSWeeklyDao dao){
      super(dao, ReportGrouping.WEEKLY, ReportGrouping.DAILY);
    }

    @Override
    public Optional<TSWeekly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<TSWeekly> findAll() {
      return dao.findAll();
    }

    @Override
    public TSWeekly save(TSWeekly entity) {
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

    public List<TableStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<TableStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTableWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
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

  public static class Monthly extends RollupStatsRepository<TSMonthly, Integer, TSMonthlyDao> {
    @Inject
    public Monthly(TSMonthlyDao dao){
      super(dao, ReportGrouping.MONTHLY, ReportGrouping.DAILY);
    }

    @Override
    public Optional<TSMonthly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<TSMonthly> findAll() {
      return dao.findAll();
    }

    @Override
    public TSMonthly save(TSMonthly entity) {
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

    public List<TableStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<TableStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTableWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
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

  public static class Quarterly extends RollupStatsRepository<TSQuarterly, Integer, TSQuarterlyDao> {
    @Inject
    public Quarterly(TSQuarterlyDao dao){
      super(dao, ReportGrouping.QUARTERLY, ReportGrouping.MONTHLY);
    }

    @Override
    public Optional<TSQuarterly> findOne(Integer id) {
      return dao.findOne(id);
    }

    @Override
    public Collection<TSQuarterly> findAll() {
      return dao.findAll();
    }

    @Override
    public TSQuarterly save(TSQuarterly entity) {
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

    public List<TableStatsDBResult> findByDatabaseAndTimeRange(Integer databaseId,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForDatabaseWithinTimeRange(databaseId, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
    }

    public List<TableStatsDBResult> findByTablesAndTimeRange(List<Integer> tableIds,
        LocalDate startDate, LocalDate endDate) {
      return dao.getAllForTableWithinTimeRange(tableIds, startDate, endDate).stream()
          .map(t -> new TableStatsDBResult(t)).collect(Collectors.toList());
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
