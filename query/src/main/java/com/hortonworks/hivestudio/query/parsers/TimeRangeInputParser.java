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
package com.hortonworks.hivestudio.query.parsers;

import java.util.HashMap;
import java.util.Map;

import com.hortonworks.hivestudio.common.orm.EntityTable;
import com.hortonworks.hivestudio.common.util.Pair;

import lombok.Value;

public class TimeRangeInputParser implements GenericParser<TimeRangeParseResult, Pair<Long, Long>>{

  private static final String START_TIME_PARAM_NAME = "startTime";
  private static final String END_TIME_PARAM_NAME = "endTime";
  private static final String TIME_EXPRESSION_TEMPLATE = "%s.start_time >= :startTime AND " +
    "%s.start_time <= :endTime"; // TODO: Generalize this to get the table columns from annotations.

  private final EntityTable hiveQueryTable;

  public TimeRangeInputParser(EntityTable hiveQueryTable) {
    this.hiveQueryTable = hiveQueryTable;
  }

  @Override
  public TimeRangeParseResult parse(Pair<Long, Long> timeRange) {
    long startTime = timeRange.getFirst();
    long endTime = timeRange.getSecond();

    Map<String, Object> parameterBindingMap = new HashMap<>();
    parameterBindingMap.put(START_TIME_PARAM_NAME, startTime);
    parameterBindingMap.put(END_TIME_PARAM_NAME, endTime);

    String expression = String.format(TIME_EXPRESSION_TEMPLATE, hiveQueryTable.getTablePrefix(), hiveQueryTable.getTablePrefix(), hiveQueryTable.getTablePrefix());

    return new TimeRangeParseResult(expression, parameterBindingMap);
  }
}
