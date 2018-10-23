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
package com.hortonworks.hivestudio.eventProcessor.dto.reporting.count;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@Getter
public class JoinCount {

  private Integer innerJoinCount = 0;
  private Integer leftOuterJoinCount = 0;
  private Integer rightOuterJoinCount = 0;
  private Integer fullOuterJoinCount = 0;
  private Integer leftSemiJoinCount = 0;
  private Integer uniqueJoinCount = 0;
  private Integer unknownJoinCount = 0;


  public JoinCount() {
  }

  public JoinCount(Integer innerJoinCount, Integer leftOuterJoinCount, Integer rightOuterJoinCount,
                   Integer fullOuterJoinCount, Integer leftSemiJoinCount, Integer uniqueJoinCount,
                   Integer unknownJoinCount) {
    this.innerJoinCount = innerJoinCount;
    this.leftOuterJoinCount = leftOuterJoinCount;
    this.rightOuterJoinCount = rightOuterJoinCount;
    this.fullOuterJoinCount = fullOuterJoinCount;
    this.leftSemiJoinCount = leftSemiJoinCount;
    this.uniqueJoinCount = uniqueJoinCount;
    this.unknownJoinCount = unknownJoinCount;
  }

  protected JoinCount copy(JoinCount otherCount) {
    return new JoinCount(
      otherCount.innerJoinCount,
      otherCount.leftOuterJoinCount,
      otherCount.rightOuterJoinCount,
      otherCount.fullOuterJoinCount,
      otherCount.leftSemiJoinCount,
      otherCount.uniqueJoinCount,
      otherCount.unknownJoinCount
    );
  }

  public Integer getTotalJoinCount() {
    return this.innerJoinCount + this.leftOuterJoinCount + this.rightOuterJoinCount + this.fullOuterJoinCount +
      this.leftSemiJoinCount + this.uniqueJoinCount + this.unknownJoinCount;
  }

  public JoinCount incrementInnerJoinCount() {
    JoinCount copy = copy(this);
    copy.innerJoinCount++;
    return copy;
  }

  public JoinCount incrementLeftOuterJoinCount() {
    JoinCount copy = copy(this);
    copy.leftOuterJoinCount++;
    return copy;
  }

  public JoinCount incrementRightOuterJoinCount() {
    JoinCount copy = copy(this);
    copy.rightOuterJoinCount++;
    return copy;
  }

  public JoinCount incrementFullOuterJoinCount() {
    JoinCount copy = copy(this);
    copy.fullOuterJoinCount++;
    return copy;
  }

  public JoinCount incrementLeftSemiJoinCount() {
    JoinCount copy = copy(this);
    copy.leftSemiJoinCount++;
    return copy;
  }

  public JoinCount incrementUniqueJoinCount() {
    JoinCount copy = copy(this);
    copy.uniqueJoinCount++;
    return copy;
  }

  public JoinCount incrementUnknownJoinCount() {
    JoinCount copy = copy(this);
    copy.unknownJoinCount++;
    return copy;
  }
}
