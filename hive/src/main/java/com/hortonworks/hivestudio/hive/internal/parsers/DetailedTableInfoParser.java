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


package com.hortonworks.hivestudio.hive.internal.parsers;

import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.DetailedTableInfo;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class DetailedTableInfoParser extends AbstractTableMetaParser<DetailedTableInfo> {
  /*
    | # Detailed Table Information  | NULL                                                                 | NULL                                                                                                                                                                                                                                                              |
    | Database:                     | default                                                              | NULL                                                                                                                                                                                                                                                              |
    | Owner:                        | admin                                                                | NULL                                                                                                                                                                                                                                                              |
    | CreateTime:                   | Mon Aug 01 13:28:42 UTC 2016                                         | NULL                                                                                                                                                                                                                                                              |
    | LastAccessTime:               | UNKNOWN                                                              | NULL                                                                                                                                                                                                                                                              |
    | Protect Mode:                 | None                                                                 | NULL                                                                                                                                                                                                                                                              |
    | Retention:                    | 0                                                                    | NULL                                                                                                                                                                                                                                                              |
    | Location:                     | hdfs://c6401.ambari.apache.org:8020/apps/hive/warehouse/geolocation  | NULL                                                                                                                                                                                                                                                              |
    | Table Type:                   | MANAGED_TABLE                                                        | NULL                                                                                                                                                                                                                                                              |
    | Table Parameters:             | NULL                                                                 | NULL                                                                                                                                                                                                                                                              |
    |                               | COLUMN_STATS_ACCURATE                                                | {\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"column1\":\"true\",\"column2\":\"true\",\"column3\":\"true\",\"column4\":\"true\",\"column5\":\"true\",\"column6\":\"true\",\"column7\":\"true\",\"column8\":\"true\",\"column9\":\"true\",\"column10\":\"true\"}}  |
    |                               | numFiles                                                             | 1                                                                                                                                                                                                                                                                 |
    |                               | numRows                                                              | 8001                                                                                                                                                                                                                                                              |
    |                               | rawDataSize                                                          | 7104888                                                                                                                                                                                                                                                           |
    |                               | totalSize                                                            | 43236                                                                                                                                                                                                                                                             |
    |                               | transient_lastDdlTime                                                | 1479819460                                                                                                                                                                                                                                                        |
    |                               | NULL                                                                 | NULL                                                                                                                                                                                                                                                              |
   */
  public DetailedTableInfoParser() {
    super("# Detailed Table Information", null, "");
  }

  @Override
  public DetailedTableInfo parse(List<Row> rows) {
    DetailedTableInfo info = new DetailedTableInfo();
    Map<String, Object> parsedSection = parseSection(rows);
    info.setDbName(getString(parsedSection, "Database:"));
    info.setOwner(getString(parsedSection, "Owner:"));
    info.setCreateTime(getString(parsedSection, "CreateTime:"));
    info.setLastAccessTime(getString(parsedSection, "LastAccessTime:"));
    info.setRetention(getString(parsedSection, "Retention:"));
    info.setLocation(getString(parsedSection, "Location:"));
    info.setTableType(getString(parsedSection, "Table Type:"));

    info.setParameters(getMap(parsedSection, "Table Parameters:"));

    return info;
  }

}
