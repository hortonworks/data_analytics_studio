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
package com.hortonworks.hivestudio.eventProcessor.processors.hive;

import java.util.HashMap;

import javax.inject.Provider;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.eventProcessor.processors.util.ProcessorHelper;
import com.hortonworks.hivestudio.eventdefs.HiveHSEvent;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;
import com.hortonworks.hivestudio.query.entities.repositories.QueryDetailsRepository;

public class TestQuerySubmittedProcessor {

  @Mock
  Provider<HiveQueryRepository> hiveQueryRepositoryProvider;
  @Mock
  Provider<QueryDetailsRepository> queryDetailsRepositoryProvider;

  ProcessorHelper helper = new ProcessorHelper(new ObjectMapper());

  QuerySubmittedProcessor querySubmittedProcessor;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    querySubmittedProcessor = new QuerySubmittedProcessor(helper,
        hiveQueryRepositoryProvider, queryDetailsRepositoryProvider);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void enrichmentFromEvent() throws Exception {
    HiveHSEvent event = new HiveHSEvent();
    QueryDetails queryDetails = new QueryDetails();
    HiveQuery hiveQuery = new HiveQuery();

    event.setOtherInfo(new HashMap<>());
    querySubmittedProcessor.enrichFromEvent(event, hiveQuery, queryDetails);
    Assert.assertEquals("Invalid CBO check, expects No", "No", hiveQuery.getUsedCBO());

    event.getOtherInfo().put("QUERY", "{\"queryText\":\"Text\", \"queryPlan\":{\"cboInfo\": \"Some text\"}}");
    querySubmittedProcessor.enrichFromEvent(event, hiveQuery, queryDetails);
    Assert.assertEquals("Invalid CBO check, expects Yes", "Yes", hiveQuery.getUsedCBO());

    event.getOtherInfo().put("QUERY", "{\"queryText\":\"Text\", \"queryPlan\":{}}");
    querySubmittedProcessor.enrichFromEvent(event, hiveQuery, queryDetails);
    Assert.assertEquals("Invalid CBO check, expects No", "No", hiveQuery.getUsedCBO());
  }

}