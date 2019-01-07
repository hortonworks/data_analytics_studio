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
import java.util.List;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Provider;

import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.common.util.TimeHelper;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnDBResult;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnResponse;
import com.hortonworks.hivestudio.reporting.entities.DBIdentifier;
import com.hortonworks.hivestudio.reporting.entities.TableIdentifier;
import com.hortonworks.hivestudio.reporting.entities.repositories.JoinColumnStatRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonthlyJoinReporter extends JoinReporter {
  private final Provider<JoinColumnStatRepository.Monthly> monthlyRepositoryProvider;

  @Inject
  public MonthlyJoinReporter(Provider<JoinColumnStatRepository.Monthly> monthlyRepositoryProvider) {
    this.monthlyRepositoryProvider = monthlyRepositoryProvider;
  }

  @Override
  protected JoinColumnResponse generateReport(DBIdentifier databaseId, LocalDate startDate, LocalDate endDate, String algorithm) {
    LocalDate monthStartDate = TimeHelper.getMonthStartDate(startDate);
    LocalDate monthEndDate = TimeHelper.getMonthEndDate(endDate);
    return generateReport(repo -> repo.findByDatabaseAndDateRange(databaseId.getId(), monthStartDate, monthEndDate, algorithm));
  }

  @Override
  protected JoinColumnResponse generateReport(TableIdentifier tableId, LocalDate startDate, LocalDate endDate) {
    LocalDate monthStartDate = TimeHelper.getMonthStartDate(startDate);
    LocalDate monthEndDate = TimeHelper.getMonthEndDate(endDate);
    return generateReport(repo -> repo.findByTableAndDateRange(tableId.getId(), monthStartDate, monthEndDate));
  }

  private JoinColumnResponse generateReport(Function<JoinColumnStatRepository.Monthly, List<JoinColumnDBResult>> dbQueryFunction) {

    JoinColumnStatRepository.Monthly monthlyRepository = monthlyRepositoryProvider.get();
    List<JoinColumnDBResult> monthStartDateToResultList = dbQueryFunction.apply(monthlyRepository);

    return calculateReport(monthStartDateToResultList);
  }
}
