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
package com.hortonworks.hivestudio.reporting.dto;

import java.time.LocalDate;
import java.util.Optional;

import com.hortonworks.hivestudio.reporting.dto.count.JoinStatsResult;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSDaily;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSMonthly;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSQuarterly;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSWeekly;

import lombok.Getter;

@Getter
public class JoinColumnDBResult {

  private LocalDate date;
  private Optional<JoinStatsResult> joinStatsOptional = Optional.empty();
  private Optional<JCSDaily> dailyOptional = Optional.empty();
  private Optional<JCSWeekly> weeklyOptional = Optional.empty();
  private Optional<JCSMonthly> monthlyOptional = Optional.empty();
  private Optional<JCSQuarterly> quarterlyOptional = Optional.empty();


  public JoinColumnDBResult(LocalDate date, JoinStatsResult result) {
    this.date = date;
    joinStatsOptional = Optional.of(result);
  }

  public JoinColumnDBResult(LocalDate date, JCSDaily result) {
    this.date = date;
    this.dailyOptional = Optional.of(result);
  }

  public JoinColumnDBResult(LocalDate date, JCSWeekly result) {
    this.date = date;
    this.weeklyOptional = Optional.of(result);
  }

  public JoinColumnDBResult(LocalDate date, JCSMonthly result) {
    this.date = date;
    this.monthlyOptional = Optional.of(result);
  }

  public JoinColumnDBResult(LocalDate date, JCSQuarterly result) {
    this.date = date;
    this.quarterlyOptional = Optional.of(result);
  }

  public JoinStatsResult toJoinStatResult() {
    if(dailyOptional.isPresent()) {
      return toJoinStatResult(dailyOptional.get());
    }

    if(weeklyOptional.isPresent()) {
      return toJoinStatResult(weeklyOptional.get());
    }

    if(monthlyOptional.isPresent()) {
      return toJoinStatResult(monthlyOptional.get());
    }

    if(quarterlyOptional.isPresent()) {
      return toJoinStatResult(quarterlyOptional.get());
    }

    return joinStatsOptional.get();

  }

  public static JoinStatsResult toJoinStatResult(JCSDaily entity) {
    return new JoinStatsResult(entity.getId(),
      entity.getLeftColumn(), entity.getLeftColumnName(),
      entity.getLeftColumnTableId(), entity.getLeftColumnTableName(), entity.getLeftColumnTableType(),
      entity.getRightColumn(), entity.getRightColumnName(),
      entity.getRightColumnTableId(), entity.getRightColumnTableName(), entity.getRightColumnTableType(),
      entity.getAlgorithm().toString(),
      entity.getInnerJoinCount(), entity.getLeftOuterJoinCount(),
      entity.getRightOuterJoinCount(), entity.getFullOuterJoinCount(),
      entity.getLeftSemiJoinCount(), entity.getUniqueJoinCount(),
      entity.getUnknownJoinCount(), entity.getTotalJoinCount());
  }

  public static JoinStatsResult toJoinStatResult(JCSWeekly entity) {
    return new JoinStatsResult(entity.getId(),
        entity.getLeftColumn(), entity.getLeftColumnName(),
        entity.getLeftColumnTableId(), entity.getLeftColumnTableName(), entity.getLeftColumnTableType(),
        entity.getRightColumn(), entity.getRightColumnName(),
        entity.getRightColumnTableId(), entity.getRightColumnTableName(), entity.getRightColumnTableType(),
        entity.getAlgorithm().toString(),
        entity.getInnerJoinCount(), entity.getLeftOuterJoinCount(),
        entity.getRightOuterJoinCount(), entity.getFullOuterJoinCount(),
        entity.getLeftSemiJoinCount(), entity.getUniqueJoinCount(),
        entity.getUnknownJoinCount(), entity.getTotalJoinCount());
  }

  public static JoinStatsResult toJoinStatResult(JCSMonthly entity) {
    return new JoinStatsResult(entity.getId(),
        entity.getLeftColumn(), entity.getLeftColumnName(),
        entity.getLeftColumnTableId(), entity.getLeftColumnTableName(), entity.getLeftColumnTableType(),
        entity.getRightColumn(), entity.getRightColumnName(),
        entity.getRightColumnTableId(), entity.getRightColumnTableName(), entity.getRightColumnTableType(),
        entity.getAlgorithm().toString(),
        entity.getInnerJoinCount(), entity.getLeftOuterJoinCount(),
        entity.getRightOuterJoinCount(), entity.getFullOuterJoinCount(),
        entity.getLeftSemiJoinCount(), entity.getUniqueJoinCount(),
        entity.getUnknownJoinCount(), entity.getTotalJoinCount());
  }

  public static JoinStatsResult toJoinStatResult(JCSQuarterly entity) {
    return new JoinStatsResult(entity.getId(),
        entity.getLeftColumn(), entity.getLeftColumnName(),
        entity.getLeftColumnTableId(), entity.getLeftColumnTableName(), entity.getLeftColumnTableType(),
        entity.getRightColumn(), entity.getRightColumnName(),
        entity.getRightColumnTableId(), entity.getRightColumnTableName(), entity.getRightColumnTableType(),
        entity.getAlgorithm().toString(),
        entity.getInnerJoinCount(), entity.getLeftOuterJoinCount(),
        entity.getRightOuterJoinCount(), entity.getFullOuterJoinCount(),
        entity.getLeftSemiJoinCount(), entity.getUniqueJoinCount(),
        entity.getUnknownJoinCount(), entity.getTotalJoinCount());
  }

}
