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
package com.hortonworks.hivestudio.reporting.services;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.hortonworks.hivestudio.reporting.ReportGrouping;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnResponse;
import com.hortonworks.hivestudio.reporting.dto.count.TableCountsReportResponse;
import com.hortonworks.hivestudio.reporting.reporter.count.CountReporter;
import com.hortonworks.hivestudio.reporting.reporter.count.CountReporterFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CountReportService extends BaseReportService {

  private final CountReporterFactory countReporterFactory;
  private final JoinReportService joinReportService;

  @Inject
  public CountReportService(CountReporterFactory countReporterFactory, JoinReportService joinReportService) {
    this.countReporterFactory = countReporterFactory;
    this.joinReportService = joinReportService;
  }

  public TableCountsReportResponse getCountsReport(Integer databaseId, LocalDate startDate, LocalDate endDate, String grouping) {
    ReportGrouping group = getReportingTimeGroup(grouping);
    CountReporter reporter = countReporterFactory.getReporter(group);
    TableCountsReportResponse response = reporter.generate(databaseId, startDate, endDate);
    return response;
  }

  public TableCountsReportResponse getCountsReportForTable(Integer tableId, LocalDate startDate, LocalDate endDate, String grouping) {
    JoinColumnResponse joinColumnResponse = joinReportService.getJoinReportForTable(tableId, startDate, endDate, grouping, "admin");
    Set<Integer> tableIds = joinColumnResponse.getResults().stream()
      .flatMap(x -> x.getJoins().stream())
      .flatMap(x -> Arrays.asList(x.getLeftTableId(), x.getRightTableId()).stream())
      .collect(Collectors.toSet());

    // Add the table for getting the result if there is no join information
    tableIds.add(tableId);

    ReportGrouping group = getReportingTimeGroup(grouping);
    CountReporter reporter = countReporterFactory.getReporter(group);
    TableCountsReportResponse response = reporter.generate(new ArrayList<>(tableIds), startDate, endDate);

    return new TableCountsReportResponse(response.getTables(), response.getReports(), joinColumnResponse.getResults());
  }
}
