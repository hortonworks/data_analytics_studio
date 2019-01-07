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
package com.hortonworks.hivestudio.eventProcessor.meta;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.SortOrder;
import com.hortonworks.hivestudio.common.entities.Table;

public class MetaInfoUtilsTest {

  private MetaInfoUtils metaInfoUtils;

  @Before
  public void setup(){
    ObjectMapper objectMapper = new ObjectMapper();
    metaInfoUtils = new MetaInfoUtils(objectMapper);
  }

  @Test
  public void fieldSchemaToColumn() {
    Date updatedTime = new Date();
    Table table = new Table();
    Boolean isBucketted = false;
    SortOrder sortOrder = SortOrder.NONE;
    boolean isPartitioned = false;

    {
      String columnName = "SomeColumn";
      String type = "STRING";
      String comment = "Some-Comment";
      FieldSchema column = new FieldSchema(columnName, type, comment);
      Column column1 = metaInfoUtils.fieldSchemaToColumn(column, 0, updatedTime, table, isBucketted, sortOrder, isPartitioned);
      Assert.assertEquals("Column name did not convert to all lowercase for mixed case input.", column1.getName(), columnName.toLowerCase());
    }
    {
      String columnName = "somecolumn";
      String type = "STRING";
      String comment = "Some-Comment";
      FieldSchema column = new FieldSchema(columnName, type, comment);
      Column column2 = metaInfoUtils.fieldSchemaToColumn(column, 1, updatedTime, table, isBucketted, sortOrder, isPartitioned);
      Assert.assertEquals("Column name did not convert to all lowercase for all lower case input.", column2.getName(), columnName.toLowerCase());
    }

    {
      String columnName = "SOMECOLUMN";
      String type = "STRING";
      String comment = "Some-Comment";
      FieldSchema column = new FieldSchema(columnName, type, comment);
      Column column3 = metaInfoUtils.fieldSchemaToColumn(column, 2, updatedTime, table, isBucketted, sortOrder, isPartitioned);
      Assert.assertEquals("Column name did not convert to all lowercase for all caps input.", column3.getName(), columnName.toLowerCase());
    }

    {
      String columnName = "Some%-#Column";
      String type = "STRING";
      String comment = "Some-Comment";
      FieldSchema column = new FieldSchema(columnName, type, comment);
      Column column4 = metaInfoUtils.fieldSchemaToColumn(column, 3, updatedTime, table, isBucketted, sortOrder, isPartitioned);
      Assert.assertEquals("Column name did not convert to all lowercase for special chars input.", column4.getName(), columnName.toLowerCase());
    }
  }

  @Test
  public void testconvertHiveTableToHSTable() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCompressed(true);
    sd.setNumBuckets(0);
    sd.setSerdeInfo(new SerDeInfo("orc", "orc.lib", null));
    sd.setLocation("/hive/test");
    sd.setInputFormat("test_input_format");
    sd.setOutputFormat("test_output_format");
    sd.setCols(Collections.emptyList());
    Map<String, String> parameters = new HashMap<>();
    parameters.put("sdkey", "sdvalue");
    sd.setParameters(parameters);
    List<FieldSchema> partitionKeys = null;
    Map<String, String> props = new HashMap<>();
    props.put("tblkey", "tblvalue");
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        new org.apache.hadoop.hive.metastore.api.Table("test_table", "test_db", "hive", 1, 2, 1,
            sd, partitionKeys, props, null, null, "EXTERNAL");
    Database database = new Database();
    database.setId(1234);
    database.setName("test_db");
    database.setCreateTime(new Date());
    database.setDropped(false);
    database.setDroppedAt(null);
    database.setCreationSource(CreationSource.EVENT_PROCESSOR);

    Table hstable = metaInfoUtils.convertHiveTableToHSTable(hiveTable, database, new Date()).getFirst();
    Assert.assertEquals("table name mismatch", "test_table", hstable.getName());
    Assert.assertEquals("db mismatch", database.getId(), hstable.getDbId());
    Assert.assertEquals("location mismatch", "/hive/test", hstable.getLocation());
    Assert.assertEquals("input format mismatch", "test_input_format", hstable.getInputFormat());
    Assert.assertEquals("output format mismatch", "test_output_format", hstable.getOutputFormat());
    Assert.assertEquals("serde mismatch", "orc.lib", hstable.getSerde());
    Assert.assertEquals("compressed mismatch", true, hstable.getCompressed());
    Assert.assertEquals("num buckets mismatch", 0, (int)hstable.getNumBuckets());
    Assert.assertEquals("sd params mismatch", "{\"sdkey\":\"sdvalue\"}", hstable.getStorageParameters().toString());
    Assert.assertEquals("sd params mismatch", "{\"tblkey\":\"tblvalue\"}", hstable.getProperties().toString());
  }
}