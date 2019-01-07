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
package com.hortonworks.hivestudio.reporting.reporter.count;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.hortonworks.hivestudio.common.util.TimeHelper;
import com.hortonworks.hivestudio.reporting.dto.count.TableCountsReportResponse;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.ColumnStatRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QuarterlyCountReporter extends CountReporter {
  private final Provider<ColumnRepository> columnRepositoryProvider;
  private final Provider<TableRepository> tableRepositoryProvider;
  private final Provider<ColumnStatRepository.Quarterly> quarterlyColumnStatsRepositoryProvider;
  private final Provider<TableStatRepository.Quarterly> quarterlyTableStatsRepositoryProvider;

  @Inject
  public QuarterlyCountReporter(Provider<ColumnRepository> columnRepositoryProvider,
                                Provider<TableRepository> tableRepositoryProvider,
                                Provider<ColumnStatRepository.Quarterly> quarterlyColumnStatsRepositoryProvider,
                                Provider<TableStatRepository.Quarterly> quarterlyTableStatsRepositoryProvider) {
    this.columnRepositoryProvider = columnRepositoryProvider;
    this.tableRepositoryProvider = tableRepositoryProvider;
    this.quarterlyColumnStatsRepositoryProvider = quarterlyColumnStatsRepositoryProvider;
    this.quarterlyTableStatsRepositoryProvider = quarterlyTableStatsRepositoryProvider;
  }

  @Override
  protected TableCountsReportResponse generateReport(Integer databaseId, LocalDate startDate, LocalDate endDate) {
    LocalDate quarterStartDate = TimeHelper.getQuarterStartDate(startDate);
    LocalDate quarterEndDate = TimeHelper.getQuarterEndDate(endDate);

    ColumnRepository columnRepository = columnRepositoryProvider.get();
    TableRepository tableRepository = tableRepositoryProvider.get();
    ColumnStatRepository.Quarterly quarterlyColumnStatsRepository = quarterlyColumnStatsRepositoryProvider.get();
    TableStatRepository.Quarterly quarterlyTableStatsRepository = quarterlyTableStatsRepositoryProvider.get();


    Map<Integer, List<Column>> allForDatabaseGroupedByTable = columnRepository.getAllForDatabaseGroupedByTable(databaseId);
    List<Table> tablesForDatabase = tableRepository.getAllForDatabase(databaseId);

    List<TableStatRepository.TableStatsDBResult> tableStats = quarterlyTableStatsRepository.findByDatabaseAndTimeRange(databaseId, quarterStartDate, quarterEndDate);
    List<ColumnStatRepository.ColumnStatsDBResult> columnStats = quarterlyColumnStatsRepository.findByDatabaseAndTimeRange(databaseId, quarterStartDate, quarterEndDate);


    return calculateTableCountsReportResponse(allForDatabaseGroupedByTable, tablesForDatabase, tableStats, columnStats);
  }

  @Override
  protected TableCountsReportResponse generateReport(List<Integer> tableIds, LocalDate startDate, LocalDate endDate) {
    LocalDate quarterStartDate = TimeHelper.getQuarterStartDate(startDate);
    LocalDate quarterEndDate = TimeHelper.getQuarterEndDate(endDate);

    ColumnRepository columnRepository = columnRepositoryProvider.get();
    TableRepository tableRepository = tableRepositoryProvider.get();
    ColumnStatRepository.Quarterly quarterlyColumnStatsRepository = quarterlyColumnStatsRepositoryProvider.get();
    TableStatRepository.Quarterly quarterlyTableStatsRepository = quarterlyTableStatsRepositoryProvider.get();


    Map<Integer, List<Column>> allForTablesGroupedByTable = columnRepository.getAllForTablesGroupedByTable(tableIds);
    List<Table> tablesForDatabase = tableRepository.getAllForTables(tableIds);

    List<TableStatRepository.TableStatsDBResult> tableStats = quarterlyTableStatsRepository.findByTablesAndTimeRange(tableIds, quarterStartDate, quarterEndDate);
    List<ColumnStatRepository.ColumnStatsDBResult> columnStats = quarterlyColumnStatsRepository.findByTablesAndTimeRange(tableIds, quarterStartDate, quarterEndDate);


    return calculateTableCountsReportResponse(allForTablesGroupedByTable, tablesForDatabase, tableStats, columnStats);
  }
}
