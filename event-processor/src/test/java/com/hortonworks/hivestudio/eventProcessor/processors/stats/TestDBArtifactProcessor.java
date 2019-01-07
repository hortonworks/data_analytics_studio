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
package com.hortonworks.hivestudio.eventProcessor.processors.stats;

import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.eventProcessor.dto.ParsedPlan;
import com.hortonworks.hivestudio.eventProcessor.dto.reporting.TableEntry;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

public class TestDBArtifactProcessor extends QueryBase {

  ParsedPlan plan;
  private DBArtifactProcessor dbArtifactProcessor;

  @Before
  public void setUp() throws Exception {
    resetQuery();
    plan = new ParsedPlan(new Query(), null, LocalDate.now());
    dbArtifactProcessor = new DBArtifactProcessor(null, null, null);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testUpdateCount() {
    ParsedPlan parsedPlan = new ParsedPlan(query, null, LocalDate.now());
    dbArtifactProcessor.updateCount(parsedPlan);

    Assert.assertEquals("Update count is invalid", 24, dbArtifactProcessor.uniqueColumns.size());
  }

  @Test
  public void updateCountTestTablesWritten() throws Exception {
    Table table1 = new Table("table1", "t1", "db1", ParsedTableType.NORMAL);
    Table table2 = new Table("table2", "t2", "db1", ParsedTableType.NORMAL);

    plan.getParsedQuery().getTablesWritten().add(table1);
    plan.getParsedQuery().getTablesWritten().add(table2);

    dbArtifactProcessor.updateCount(plan);
    Assert.assertEquals("Invalid number of unique DBs", 1, dbArtifactProcessor.uniqueDatabases.size());
    Assert.assertTrue("Invalid database name", dbArtifactProcessor.uniqueDatabases.contains("db1"));

    Assert.assertEquals("Invalid number of unique tables", 2, dbArtifactProcessor.uniqueTables.size());
    for (TableEntry uniqueTable : dbArtifactProcessor.uniqueTables) {
      String tableName = uniqueTable.getTableName();
      Assert.assertTrue("Invalid table name", tableName.equals("table1") || tableName.equals("table2"));
    }
  }

}