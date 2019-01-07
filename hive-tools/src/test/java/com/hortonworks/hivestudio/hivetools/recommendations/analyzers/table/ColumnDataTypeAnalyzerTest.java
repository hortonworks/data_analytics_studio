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
package com.hortonworks.hivestudio.hivetools.recommendations.analyzers.table;

import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ColumnDataTypeAnalyzerTest {

  ColumnDataTypeAnalyzer columnDataTypeAnalyzer;

  @Before
  public void setUp() throws Exception {
    columnDataTypeAnalyzer = new ColumnDataTypeAnalyzer();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void normalizeDataType() throws Exception {
    Assert.assertEquals("Didn't handle null", null, columnDataTypeAnalyzer.normalizeDataType(null));
    Assert.assertEquals("Didn't convert to lower case", "type", columnDataTypeAnalyzer.normalizeDataType("TYPE"));
    Assert.assertEquals("Didn't extract array", "array", columnDataTypeAnalyzer.normalizeDataType("array<double>"));
  }

  @Test
  public void analyzeColumn() throws Exception {
    TableMeta meta = new TableMeta();
    meta.setTable("T1");

    ColumnInfo info = new ColumnInfo();
    info.setName("Col1");

    // String type recommendations
    info.setType("VARCHAR");
    String expectedMsg = String.format(ColumnDataTypeAnalyzer.STR_TYPE_MSG, info.getType(), meta.getTable(), info.getName());
    Assert.assertEquals("Invalid recommendation for " + info.getType(), expectedMsg, columnDataTypeAnalyzer.analyzeColumn(meta, info).get(0).getMessage());

    info.setType("CHAR");
    expectedMsg = String.format(ColumnDataTypeAnalyzer.STR_TYPE_MSG, info.getType(), meta.getTable(), info.getName());
    Assert.assertEquals("Invalid recommendation for " + info.getType(), expectedMsg, columnDataTypeAnalyzer.analyzeColumn(meta, info).get(0).getMessage());

    info.setType("STRING");
    Assert.assertEquals("Recommendation shouldn't be generated for " + info.getType(), 0, columnDataTypeAnalyzer.analyzeColumn(meta, info).size());

    // Decimal type recommendations
    info.setType("decimal");
    info.setPrecision(10);
    info.setScale(0);
    expectedMsg = String.format(ColumnDataTypeAnalyzer.DECIMAL_TO_INT_MSG, meta.getTable(), info.getName());
    Assert.assertEquals("Invalid recommendation for " + info.getType(), expectedMsg, columnDataTypeAnalyzer.analyzeColumn(meta, info).get(0).getMessage());

    info.setPrecision(20);
    Assert.assertEquals("Recommendation shouldn't be generated for " + info.getType(), 0, columnDataTypeAnalyzer.analyzeColumn(meta, info).size());

    info.setPrecision(10);
    info.setScale(1);
    Assert.assertEquals("Recommendation shouldn't be generated for " + info.getType(), 0, columnDataTypeAnalyzer.analyzeColumn(meta, info).size());

    // Complex type recommendations
    info.setType("array<double>");
    expectedMsg = String.format(ColumnDataTypeAnalyzer.COMPLEX_TYPE_MSG, meta.getTable(), info.getName());
    Assert.assertEquals("Invalid recommendation for " + info.getType(), expectedMsg, columnDataTypeAnalyzer.analyzeColumn(meta, info).get(0).getMessage());

    info.setType("map");
    expectedMsg = String.format(ColumnDataTypeAnalyzer.COMPLEX_TYPE_MSG, meta.getTable(), info.getName());
    Assert.assertEquals("Invalid recommendation for " + info.getType(), expectedMsg, columnDataTypeAnalyzer.analyzeColumn(meta, info).get(0).getMessage());

    info.setType("struct");
    expectedMsg = String.format(ColumnDataTypeAnalyzer.COMPLEX_TYPE_MSG, meta.getTable(), info.getName());
    Assert.assertEquals("Invalid recommendation for " + info.getType(), expectedMsg, columnDataTypeAnalyzer.analyzeColumn(meta, info).get(0).getMessage());

    info.setType("uniontype");
    expectedMsg = String.format(ColumnDataTypeAnalyzer.COMPLEX_TYPE_MSG, meta.getTable(), info.getName());
    Assert.assertEquals("Invalid recommendation for " + info.getType(), expectedMsg, columnDataTypeAnalyzer.analyzeColumn(meta, info).get(0).getMessage());
  }

}