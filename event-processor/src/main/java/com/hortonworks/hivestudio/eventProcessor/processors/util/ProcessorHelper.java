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
package com.hortonworks.hivestudio.eventProcessor.processors.util;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveEventType;
import com.hortonworks.hivestudio.eventProcessor.processors.TezEventType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessorHelper {

  private final ObjectMapper objectMapper;

  @Inject
  public ProcessorHelper(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public ArrayNode processTablesReadWrite(List<String> tables) {
    if (tables == null) {
      return objectMapper.createArrayNode();
    }
    List<ObjectNode> tableNodes = tables.stream().map(x -> {
      ObjectNode table = objectMapper.createObjectNode();
      String[] split = x.split("\\.");
      table.put("database", split[0]);
      table.put("table", split[1]);
      return table;
    }).collect(Collectors.toList());

    return objectMapper.createArrayNode().addAll(tableNodes);
  }

  public <T> T parseData(String data, Class<T> klass) {
    if(data == null || klass == null) {
      return null;
    }
    try {
      return objectMapper.readValue(data, klass);
    } catch (IOException e) {
      log.error("Failed to parse data: {}", e);
      return null;
    }
  }

  public boolean isValidEvent(String extractedEventType, HiveEventType... validEventType) {
    try {
      return Sets.newHashSet(validEventType).contains(HiveEventType.valueOf(extractedEventType.toUpperCase()));
    } catch (IllegalArgumentException ex) {
      return false;
    }
  }

  public boolean isValidEvent(String extractedEventType, TezEventType... validEventType) {
    try {
      return Sets.newHashSet(validEventType).contains(TezEventType.valueOf(extractedEventType.toUpperCase()));
    } catch (IllegalArgumentException ex) {
      return false;
    }
  }
}
