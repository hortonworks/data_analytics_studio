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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hortonworks.hivestudio.reporting.dto.count.ColumnStatsResult;
import com.hortonworks.hivestudio.reporting.dto.count.TableCountsReport;
import com.hortonworks.hivestudio.reporting.dto.count.TableCountsReportResponse;
import com.hortonworks.hivestudio.reporting.dto.count.TableInfoResult;
import com.hortonworks.hivestudio.reporting.dto.count.TableStatsResult;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.reporting.entities.repositories.ColumnStatRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository;
import com.hortonworks.hivestudio.reporting.reporter.BaseReporter;

public abstract class CountReporter extends BaseReporter {
  public TableCountsReportResponse generate(Integer databaseId, LocalDate startDate, LocalDate endDate) {
    assertDate(startDate, endDate);
    return generateReport(databaseId, startDate, endDate);
  }

  public TableCountsReportResponse generate(List<Integer> tableIds, LocalDate startDate, LocalDate endDate) {
    assertDate(startDate, endDate);
    return generateReport(tableIds, startDate, endDate);
  }


  protected abstract TableCountsReportResponse generateReport(Integer databaseId, LocalDate startDate, LocalDate endDate);
  protected abstract TableCountsReportResponse generateReport(List<Integer> tableIds, LocalDate startDate, LocalDate endDate);

  protected Map<LocalDate, List<TableStatsResult>> getTableStatsResultMap(List<TableStatRepository.TableStatsDBResult> tableStats, List<ColumnStatRepository.ColumnStatsDBResult> columnStats) {
    Map<LocalDate, Map<Integer, List<ColumnStatsResult>>> groupedColumnStats = getGroupedColumnStatResults(columnStats);

    Map<LocalDate, List<TableStatsResult>> tableResultMap = new HashMap<>();

    for(TableStatRepository.TableStatsDBResult dbResult: tableStats) {
      LocalDate date = dbResult.getDate();
      Integer tableId = dbResult.getTableId();
      List<TableStatsResult> tableStatsList = tableResultMap.get(date);

      List<ColumnStatsResult> columnStatsResultList = groupedColumnStats
        .getOrDefault(date, new HashMap<>())
        .getOrDefault(tableId, new ArrayList<>());

      TableStatsResult result = new TableStatsResult(tableId, dbResult.getReadCount(),
          dbResult.getWriteCount(), dbResult.getBytesRead(), dbResult.getRecordsRead(),
          dbResult.getBytesWritten(), dbResult.getRecordsWritten(), columnStatsResultList);

      if(tableStatsList == null) {
        tableStatsList = new ArrayList<>();

        tableStatsList.add(result);
        tableResultMap.put(date, tableStatsList);
        continue;
      }

      tableStatsList.add(result);
    }
    return tableResultMap;
  }

  /*
   * Create a Map(Date => Map(TableId => ColumnStatsResult))
   */
  private Map<LocalDate, Map<Integer, List<ColumnStatsResult>>> getGroupedColumnStatResults(List<ColumnStatRepository.ColumnStatsDBResult> columnStats) {
    Map<LocalDate, Map<Integer, List<ColumnStatsResult>>> result = new HashMap<>();

    for(ColumnStatRepository.ColumnStatsDBResult dbResult : columnStats) {
      LocalDate date = dbResult.getDate();
      Map<Integer, List<ColumnStatsResult>> columnStatResult = result.get(date);

      if(columnStatResult == null) {
        columnStatResult = new HashMap<>();
        List<ColumnStatsResult> columnStatsList = new ArrayList<>();
        columnStatsList.add(dbResult.getStat());
        columnStatResult.put(dbResult.getTableId(), columnStatsList);
        result.put(date, columnStatResult);
        continue;
      }

      List<ColumnStatsResult> columnStatsList = columnStatResult.get(dbResult.getTableId());
      if(columnStatsList == null) {
        columnStatsList = new ArrayList<>();
        columnStatsList.add(dbResult.getStat());
        columnStatResult.put(dbResult.getTableId(), columnStatsList);
        continue;
      }

      columnStatsList.add(dbResult.getStat());
    }
    return result;
  }

  protected TableCountsReportResponse calculateTableCountsReportResponse(Map<Integer, List<Column>> columnsGroupedByTable, List<Table> tables, List<TableStatRepository.TableStatsDBResult> tableStats, List<ColumnStatRepository.ColumnStatsDBResult> columnStats) {
    List<TableInfoResult> tableInfoResults = tables.stream()
      .map(x -> TableInfoResult.fromTableAndColumns(x, columnsGroupedByTable.get(x.getId())))
      .collect(Collectors.toList());

    Map<LocalDate, List<TableStatsResult>> tableStatsResultsMap = getTableStatsResultMap(tableStats, columnStats);

    List<TableCountsReport> countReports = tableStatsResultsMap.keySet()
      .stream()
      .map(x -> new TableCountsReport(x, tableStatsResultsMap.get(x)))
      .collect(Collectors.toList());

    return new TableCountsReportResponse(tableInfoResults, countReports, null);
  }

}
