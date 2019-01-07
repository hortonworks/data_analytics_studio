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
package com.hortonworks.hivestudio.reporting.reporter.join;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.common.exception.generic.ConstraintViolationException;
import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.reporting.dto.ColumnResult;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnDBResult;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnRelationship;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnResponse;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnResultEntry;
import com.hortonworks.hivestudio.reporting.dto.TableResult;
import com.hortonworks.hivestudio.reporting.dto.count.JoinStatsResult;
import com.hortonworks.hivestudio.reporting.entities.DBIdentifier;
import com.hortonworks.hivestudio.reporting.entities.TableIdentifier;
import com.hortonworks.hivestudio.reporting.reporter.BaseReporter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class JoinReporter extends BaseReporter {

  public JoinColumnResponse generate(DBIdentifier databaseId, LocalDate startDate, LocalDate endDate, String algorithm) {
    assertDate(startDate, endDate);
    return generateReport(databaseId, startDate, endDate, algorithm);
  }

  public JoinColumnResponse generate(TableIdentifier tableId, LocalDate startDate, LocalDate endDate) {
    assertDate(startDate, endDate);
    return generateReport(tableId, startDate, endDate);
  }

  protected abstract JoinColumnResponse generateReport(DBIdentifier databaseId, LocalDate startDate, LocalDate endDate, String algorithm);
  protected abstract JoinColumnResponse generateReport(TableIdentifier tableId, LocalDate startDate, LocalDate endDate);

  protected Pair<Set<TableResult>, Set<ColumnResult>> getTablesAndColumnsData(List<JoinColumnDBResult> allJoinColumnDbResult) {
    Set<TableResult> tables = new HashSet<>();
    Set<ColumnResult> columns = new HashSet<>();

    allJoinColumnDbResult.forEach(x -> {
      JoinStatsResult result = x.toJoinStatResult();
      TableResult leftTableResult = new TableResult(result.getLeftTableId(), result.getLeftTableName(), result.getLeftTableType());
      TableResult rightTableResult = new TableResult(result.getRightTableId(), result.getRightTableName(), result.getRightTableType());
      tables.add(leftTableResult);
      tables.add(rightTableResult);

      ColumnResult leftColumnResult = new ColumnResult(result.getLeftColumnId(), result.getLeftColumnName());
      ColumnResult rightColumnResult = new ColumnResult(result.getRightColumnId(), result.getRightColumnName());
      columns.add(leftColumnResult);
      columns.add(rightColumnResult);
    });

    return new Pair<>(tables, columns);
  }

  protected List<JoinColumnResultEntry> getResultEntryFromGroupedData(Map<LocalDate, List<JoinColumnDBResult>> groupedByDate, Function<LocalDate, JoinColumnResultEntry> resultEntryFunction) {
    return groupedByDate.keySet()
      .stream()
      .map(resultEntryFunction).collect(Collectors.toList());
  }

  protected Map<LocalDate, List<JoinColumnDBResult>> groupDBResultByDate(List<JoinColumnDBResult> weekStartDateToDbResultList) {
    return weekStartDateToDbResultList
      .stream()
      .collect(
        Collectors.groupingBy(JoinColumnDBResult::getDate, Collectors.mapping(Function.identity(), Collectors.toList()))
      );
  }

  protected List<JoinColumnDBResult> getAllDBResultFromGroup(Map<LocalDate, List<JoinColumnDBResult>> groupedByDate) {
    return groupedByDate.values()
      .stream()
      .flatMap(List::stream)
      .collect(Collectors.toList());
  }

  protected JoinColumnResponse calculateReport (List<JoinColumnDBResult> dbResult) {
    Map<LocalDate, List<JoinColumnDBResult>> groupedByDate = groupDBResultByDate(dbResult);

    ArrayList<JoinColumnResultEntry> directLinks = new ArrayList<>();
    ArrayList<JoinColumnResultEntry> intermediateLinks = new ArrayList<>();

    for (LocalDate localDate : groupedByDate.keySet()) {

      ArrayList<JoinStatsResult> directLinkResults = new ArrayList<>();
      ArrayList<JoinStatsResult> intermediateLinkResults = new ArrayList<>();

      for (JoinColumnDBResult joinColumnDBResult : groupedByDate.get(localDate)) {

        JoinStatsResult result = joinColumnDBResult.toJoinStatResult();
        if(result.getLeftTableType().equals(ParsedTableType.INTERMEDIATE.toString())) {
          intermediateLinkResults.add(result);
        }
        else {
          directLinkResults.add(result);
        }
      }

      directLinks.add(new JoinColumnResultEntry(localDate, directLinkResults));
      intermediateLinks.add(new JoinColumnResultEntry(localDate, intermediateLinkResults));
    }

    List<JoinColumnDBResult> allJoinColumnDbResult = getAllDBResultFromGroup(groupedByDate);
    Pair<Set<TableResult>, Set<ColumnResult>> tablesAndColumnsData = getTablesAndColumnsData(allJoinColumnDbResult);
    Set<TableResult> tables = tablesAndColumnsData.getFirst();
    Set<ColumnResult> columns = tablesAndColumnsData.getSecond();
    JoinColumnRelationship relationship = new JoinColumnRelationship(new ArrayList<>(tables), new ArrayList<>(columns));

    return new JoinColumnResponse(directLinks, intermediateLinks, relationship);
  }
}
