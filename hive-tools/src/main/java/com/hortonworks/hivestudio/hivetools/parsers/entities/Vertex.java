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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.hivetools.parsers.QueryPlanParser;

import java.util.ArrayList;
import java.util.HashMap;

public class Vertex {

  public enum Type {
    MAPPER, REDUCER, UNKNOWN;
  }

  private String name;
  private Type type;
  private Table table; // Table on which scan is performed

  private JsonNode details;

  private ArrayList<Vertex> incomingVertices;
  private HashMap<String, Column> columnsScanned;
  private HashMap<String, Expression> expressionsMap;

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public JsonNode getDetails() {
    return details;
  }

  public ArrayList<Vertex> getIncomingVertices() {
    return incomingVertices;
  }

  public HashMap<String, Column> getColumnsScanned() {
    return columnsScanned;
  }

  public HashMap<String, Expression> getExpressionsMap() {
    return expressionsMap;
  }

  public void setExpressionsMap(HashMap<String, Expression> expressions) {
    this.expressionsMap = expressions;
  }

  public Table getTable() {
    return table;
  }

  public Vertex(String name, JsonNode details) {
    this.name = name;
    this.details = details;

    this.type = extractType();
    this.table = extractTable();

    this.incomingVertices = new ArrayList<>();
    this.columnsScanned = new HashMap<>();
  }

  private Type extractType() {
    if(this.name.startsWith("Map")) {
      return Type.MAPPER;
    }
    else if(this.name.startsWith("Reducer")) {
      return Type.REDUCER;
    }
    return Type.UNKNOWN;
  }

  private Boolean getBoolValue(JsonNode node, String childName) {
    JsonNode child = node.get(childName);
    if(child != null) {
      if(child instanceof TextNode){
        return Boolean.parseBoolean(child.textValue());
      }
      return child.booleanValue();
    }
    return false;
  }

  private String getTextValue(JsonNode node, String childName) {
    JsonNode child = node.get(childName);
    if(child != null) {
      return child.textValue();
    }
    return null;
  }

  private Table extractTable() {
    JsonNode operationTree = getOperatorTree();
    String tableScanKey = QueryPlanParser.OperatorType.TABLE_SCAN.toString();

    if(operationTree.has(tableScanKey)) {
      JsonNode tableScan = operationTree.get(tableScanKey);
      ParsedTableType tableType = getBoolValue(tableScan,"isTempTable:") ? ParsedTableType.TEMP : ParsedTableType.NORMAL;

      return new Table(getTextValue(tableScan,"table:"),
        getTextValue(tableScan,"alias:"),
        getTextValue(tableScan,"database:"),
        tableType);
    }

    return null;
  }

  public JsonNode getOperatorTree() {
    JsonNode operatorTree = JsonNodeFactory.instance.objectNode();

    if(type == Type.MAPPER) {
      operatorTree = details.get("Map Operator Tree:").get(0);
    }
    else if(type == Type.REDUCER) {
      operatorTree = details.get("Reduce Operator Tree:");
    }

    return operatorTree;
  }

  public HashMap<String, Vertex> getIncomingVertexIndexHash(JsonNode inputVertices) {
    HashMap<String, Vertex> vertices = new HashMap<>();

    // Must be a mapper
    if(inputVertices != null) {
      HashMap<String, Vertex> vertexNameMap = new HashMap<>();
      this.getIncomingVertices().forEach(vertex->{
        vertexNameMap.put(vertex.getName(), vertex);
      });

      inputVertices.fields().forEachRemaining(vertex->{
        vertices.put(vertex.getKey(), vertexNameMap.get(vertex.getValue().textValue()));
      });

      // Guess the current vertex index!
      String currentVertexIndex = vertices.containsKey("0") ? String.valueOf(vertices.size()) : "0";
      vertices.put(currentVertexIndex, this);
    }
    // Must be a reducer
    else {
      this.getIncomingVertices().forEach(vertex->{
        vertices.put(String.valueOf(vertices.size()), vertex);
      });
    }

    return vertices;
  }

}
