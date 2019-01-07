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
package com.hortonworks.hivestudio.hivetools.parsers;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Table;

public class TestQueryPlanParser {
  private static ObjectMapper mapper = new ObjectMapper();

  // Todo: Must move out to a test-helpers module
  private static ObjectNode loadFromResource(String resourceName) throws Exception {
    return (ObjectNode) mapper.readTree(
        TestQueryPlanParser.class.getClassLoader().getResourceAsStream(resourceName));
  }

  private static ArrayNode makeTablesArrayNode(String database, String ... tables) {
    ArrayNode node = mapper.createArrayNode();
    for (String table : tables) {
      ObjectNode val = mapper.createObjectNode();
      val.put("database", database);
      val.put("table", table);
      node.add(val);
    }
    return node;
  }

  private static final String EXPLAIN_PLAN_RESOURCE_1 = "test_explain_plan1.json";

  @Test
  public void testQueryPlanParser() throws Exception {
    QueryPlanParser parser = new QueryPlanParser();
    Query plan = parser.parse(loadFromResource(EXPLAIN_PLAN_RESOURCE_1),
        makeTablesArrayNode("default", "t1", "t2"));
    for (Table table : plan.getTablesWritten()) {
      Assert.assertEquals("Database is wrong", "default", table.getDatabaseName());
      Assert.assertTrue("Unexpected table name",
          "t1".equals(table.getName()) || "t2".equals(table.getName()));
    }

    Assert.assertEquals("Num projections not correct", 61, plan.getProjections().size());
    Assert.assertEquals("Num joins not correct", 6, plan.getJoins().size());
    Assert.assertEquals("Num filters not correct", 7, plan.getFilters().size());
    Assert.assertEquals("Num aggregations not correct", 14, plan.getAggregations().size());
  }

}
