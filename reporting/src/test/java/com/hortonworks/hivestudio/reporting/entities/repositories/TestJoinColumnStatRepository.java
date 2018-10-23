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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.time.LocalDate;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.result.ResultIterable;
import org.jdbi.v3.core.result.ResultIterator;
import org.jdbi.v3.core.statement.Query;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.hortonworks.hivestudio.reporting.dao.JCSDailyDao;
import com.hortonworks.hivestudio.reporting.dto.count.JoinStatsResult;

@Ignore
public class TestJoinColumnStatRepository {

  @Mock Handle handle;
  @Mock Query query;
  @Mock ResultIterator<JoinStatsResult> iterator;
  @Mock JCSDailyDao dao;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(dao.getHandle()).thenReturn(handle);
    when(handle.createQuery(anyString())).thenReturn(query);
    // TODO: query.bind is final and hence cannot be mocked.
    // when(query.bind(anyString(), anyString())).thenReturn(query);
    // when(query.bind(anyString(), anyInt())).thenReturn(query);
    when(query.mapTo(eq(JoinStatsResult.class))).thenReturn(new ResultIterable<JoinStatsResult>() {
      @Override
      public ResultIterator<JoinStatsResult> iterator() {
        return iterator;
      }
    });
  }

  @Test
  public void dailyFindByDatabaseAndDateRange() throws Exception {
    JoinColumnStatRepository.Daily daily = new JoinColumnStatRepository.Daily(dao);
    LocalDate now = LocalDate.now();

    daily.findByDatabaseAndDateRange(0, now, now, "alg");
    String expectedQuery = String.format(JoinColumnStatRepository.GET_FOR_DB_ALG_FORMAT, "daily");
    Mockito.verify(handle, Mockito.times(1)).createQuery(expectedQuery);

    daily.findByDatabaseAndDateRange(0, now, now, "");
    expectedQuery = String.format(JoinColumnStatRepository.GET_FOR_DB_FORMAT, "daily");
    Mockito.verify(handle, Mockito.times(1)).createQuery(expectedQuery);

    daily.findByDatabaseAndDateRange(0, now, now, null);
    expectedQuery = String.format(JoinColumnStatRepository.GET_FOR_TABLE_FORMAT, "daily");
    Mockito.verify(handle, Mockito.times(2)).createQuery(expectedQuery);
  }
}