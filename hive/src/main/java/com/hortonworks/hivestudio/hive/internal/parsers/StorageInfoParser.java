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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnOrder;
import com.hortonworks.hivestudio.hive.internal.dto.Order;
import com.hortonworks.hivestudio.hive.internal.dto.StorageInfo;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the Storage Information from the describe formatted output.
 */
@Slf4j
public class StorageInfoParser extends AbstractTableMetaParser<StorageInfo> {
  public StorageInfoParser() {
    super("# Storage Information", null, "");
  }

  @Override
  public StorageInfo parse(List<Row> rows) {
    StorageInfo info = new StorageInfo();
    Map<String, Object> parsedSection = parseSection(rows);

    info.setSerdeLibrary(getString(parsedSection, "SerDe Library:"));
    info.setInputFormat(getString(parsedSection, "InputFormat:"));
    info.setOutputFormat(getString(parsedSection, "OutputFormat:"));
    info.setCompressed(getString(parsedSection, "Compressed:"));
    info.setNumBuckets(getString(parsedSection, "Num Buckets:"));
    info.setBucketCols(parseBucketColumns(getString(parsedSection, "Bucket Columns:")));
    info.setSortCols(parseSortCols(getString(parsedSection, "Sort Columns:")));
    info.setParameters(getMap(parsedSection, "Storage Desc Params:"));

    return info;
  }

  private List<String> parseBucketColumns(String string) {
    String[] strings = string.split("[\\[\\],]");
    return FluentIterable.from(Arrays.asList(strings)).filter(new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String input) {
        return !(null == input || input.trim().length() == 0) ;
      }
    }).transform(new Function<String, String>() {
      @Override
      public String apply(String input) {
        return input.trim();
      }
    }).toList();
  }

  private List<ColumnOrder> parseSortCols(String str) {
    String patternStr = "Order\\s*\\(\\s*col\\s*:\\s*([^,]+)\\s*,\\s*order\\s*:\\s*(\\d)\\s*\\)";
    Pattern pattern = Pattern.compile(patternStr);

    Matcher matcher = pattern.matcher(str);

    LinkedList<ColumnOrder> list = new LinkedList<>();
    while(matcher.find()){
      String colName = matcher.group(1);
      String orderString = matcher.group(2);
      Order order = Order.fromOrdinal(Integer.valueOf(orderString));
      ColumnOrder co = new ColumnOrder(colName, order);
      list.add(co);
      log.debug("columnOrder : {}", co);
    }

    return list;
  }
}
