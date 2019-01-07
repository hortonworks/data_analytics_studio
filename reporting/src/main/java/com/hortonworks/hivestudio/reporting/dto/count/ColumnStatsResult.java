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
package com.hortonworks.hivestudio.reporting.dto.count;

import com.hortonworks.hivestudio.reporting.entities.columnstat.CSDaily;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSMonthly;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSQuarterly;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSWeekly;

import lombok.Getter;

@Getter
public class ColumnStatsResult {
  private final Integer id;
  private final Integer joinCount;
  private final Integer filterCount;
  private final Integer aggregationCount;
  private final Integer projectionCount;

  public ColumnStatsResult(CSDaily daily) {
    this(daily.getColumnId(), daily.getJoinCount(), daily.getFilterCount(),
        daily.getAggregationCount(), daily.getProjectionCount());
  }

  public ColumnStatsResult(CSWeekly weekly) {
    this(weekly.getColumnId(), weekly.getJoinCount(), weekly.getFilterCount(),
        weekly.getAggregationCount(), weekly.getProjectionCount());
  }

  public ColumnStatsResult(CSMonthly monthly) {
    this(monthly.getColumnId(), monthly.getJoinCount(), monthly.getFilterCount(),
        monthly.getAggregationCount(), monthly.getProjectionCount());
  }

  public ColumnStatsResult(CSQuarterly quarterly) {
    this(quarterly.getColumnId(), quarterly.getJoinCount(), quarterly.getFilterCount(),
        quarterly.getAggregationCount(), quarterly.getProjectionCount());
  }

  private ColumnStatsResult(Integer id, Integer joinCount, Integer filterCount,
      Integer aggregationCount, Integer projectionCount) {
    this.id = id;
    this.joinCount = joinCount;
    this.filterCount = filterCount;
    this.aggregationCount = aggregationCount;
    this.projectionCount = projectionCount;
  }
}
