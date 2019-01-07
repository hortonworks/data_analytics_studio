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
package com.hortonworks.hivestudio.hivetools.parsers.entities;

import com.hortonworks.hivestudio.hivetools.QueryBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.util.List;

public class JoinTest extends QueryBase {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testExtractLinkSingleColEqui() throws Exception {
    Query query = loadQuery("test_explain_plan2.json");

    List<Join> joins = query.getJoins();

    JoinLink join = joins.get(0).extractLink();
    Assert.assertEquals("Invalid join column", "cs_sold_date_sk", join.getLeftColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "catalog_sales", join.getLeftColumn().getTable().getName());
    Assert.assertEquals("Invalid join column", "d_date_sk", join.getRightColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "date_dim", join.getRightColumn().getTable().getName());

    join = joins.get(1).extractLink();
    Assert.assertEquals("Invalid join column", "cs_bill_customer_sk", join.getLeftColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "catalog_sales", join.getLeftColumn().getTable().getName());
    Assert.assertEquals("Invalid join column", "c_customer_sk", join.getRightColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "customer", join.getRightColumn().getTable().getName());

    join = joins.get(2).extractLink();
    Assert.assertEquals("Invalid join column", "c_current_addr_sk", join.getLeftColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "customer", join.getLeftColumn().getTable().getName());
    Assert.assertEquals("Invalid join column", "ca_address_sk", join.getRightColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "customer_address", join.getRightColumn().getTable().getName());
  }

  @Test
  public void testExtractLinkSimpleExp() throws Exception {
    Query query = loadQuery("test_explain_plan3.json");

    List<Join> joins = query.getJoins();

    JoinLink join = joins.get(0).extractLink();
    Assert.assertEquals("Invalid join column", "(cs_ext_sales_price + cs_sales_price)", join.getLeftColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "catalog_sales", join.getLeftColumn().getTable().getName());
    Assert.assertEquals("Invalid join column", "p_cost", join.getRightColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "promotion", join.getRightColumn().getTable().getName());
  }

  @Test
  public void testExtractLinkCompositKey() throws Exception {
    Query query = loadQuery("test_explain_plan4.json");

    List<Join> joins = query.getJoins();

    //cs_ext_sales_price + cs_sales_price = p_cost
    JoinLink join = joins.get(0).extractLink();
    Assert.assertEquals("Invalid join column", "(cs_sales_price, cs_ext_sales_price)", join.getLeftColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "catalog_sales", join.getLeftColumn().getTable().getName());
    Assert.assertEquals("Invalid join column", "(p_cost, p_cost)", join.getRightColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "promotion", join.getRightColumn().getTable().getName());

    // Composit column key order test, automatically ordered by Hive
    query = loadQuery("test_explain_plan5.json");

    joins = query.getJoins();

    //cs_ext_sales_price + cs_sales_price = p_cost
    join = joins.get(0).extractLink();
    Assert.assertEquals("Invalid join column", "(cs_sales_price, cs_ext_sales_price)", join.getLeftColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "catalog_sales", join.getLeftColumn().getTable().getName());
    Assert.assertEquals("Invalid join column", "(p_cost, p_cost)", join.getRightColumn().getColumnName());
    Assert.assertEquals("Invalid join table", "promotion", join.getRightColumn().getTable().getName());
  }

  @Test
  public void testStandardizeAlgType() throws Exception {

    Assert.assertEquals(Join.AlgorithmType.SHUFFLE_JOIN, Join.standardizeAlgType(Join.AlgorithmType.JOIN));
    Assert.assertEquals(Join.AlgorithmType.HASH_JOIN, Join.standardizeAlgType(Join.AlgorithmType.MAP_JOIN));
    Assert.assertEquals(Join.AlgorithmType.SMB_JOIN, Join.standardizeAlgType(Join.AlgorithmType.SMB_MAP_JOIN));
    Assert.assertEquals(Join.AlgorithmType.MERGE_JOIN, Join.standardizeAlgType(Join.AlgorithmType.MERGE_JOIN));

  }

}