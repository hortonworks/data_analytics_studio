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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Expression;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Join;
import com.hortonworks.hivestudio.hivetools.parsers.entities.JoinDefinition;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Query;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Vertex;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Column;
import com.hortonworks.hivestudio.hivetools.parsers.entities.EnumVals;
import com.hortonworks.hivestudio.hivetools.parsers.entities.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class QueryPlanParser {

  private Utils utils;

  public QueryPlanParser() {
    this.utils = new Utils();
  }

  public enum OperatorType {
    TABLE_SCAN("TableScan"),

    SELECT("Select Operator"),
    FILTER("Filter Operator"),
    GROUP_BY("Group By Operator"),

    REDUCE_OUTPUT("Reduce Output Operator"),
    FILE_OUTPUT("File Output Operator"),

    JOIN("Join Operator");

    public static final EnumVals<QueryPlanParser.OperatorType> vals = new EnumVals<>(values());
    private final String value;
    OperatorType(String val) {
      value = val;
    }
    public String toString() {
      return value;
    }
  }

  private ArrayList<Expression> createExpressions(List<String> expressionStrs, Vertex vertex, HashMap<String, Expression> sourceExpressionMap, JsonNode operatorObject) {
    ArrayList<Expression> expressions = new ArrayList<>();

    for (String expressionStr : expressionStrs) {
      expressions.add(new Expression(expressionStr.trim(), vertex, sourceExpressionMap, operatorObject));
    }

    return expressions;
  }

  private ArrayList<Expression> extractExpressions(String expressionsStr, Vertex vertex, HashMap<String, Expression> sourceExpressionMap, JsonNode operatorObject) {    
    return createExpressions(Arrays.asList((expressionsStr + ",").split("\\(type:.*?\\),")), vertex, sourceExpressionMap, operatorObject);
  }

  private void extractScanInfo(JsonNode operatorObject, Query query, Vertex vertex) {
    JsonNode columnNames = operatorObject.get("columns:");
    Table table = vertex.getTable();

    if(columnNames != null && table != null) {
      columnNames.forEach(columnName->{
        String columnNameStr = columnName.textValue().toLowerCase();
        Column column = new Column(columnNameStr, table);

        query.getScans().add(column); // TODO: Instead of collating it at this level for a query. Probably we can collate it from all the vertices under a query
        vertex.getColumnsScanned().put(columnNameStr, column);
      });
    }
  }

  private void extractSelectInfo(JsonNode operatorObject, Query query, Vertex vertex, HashMap<String, Expression> sourceExpressionMap) {
    String expressions = operatorObject.get("expressions:").textValue();
    query.getProjections().addAll(extractExpressions(expressions, vertex, sourceExpressionMap, operatorObject));
  }

  private void extractFilterInfo(JsonNode operatorObject, Query query, Vertex vertex, HashMap<String, Expression> sourceExpressionMap) {
    String predicate = operatorObject.get("predicate:").textValue();
    query.getFilters().addAll(extractExpressions(predicate, vertex, sourceExpressionMap, operatorObject));
  }

  private void extractGroupByInfo(JsonNode operatorObject, Query query, Vertex vertex, HashMap<String, Expression> sourceExpressionMap) {
    ArrayList<Expression> expressions;
    if(operatorObject.has("aggregations:")) {
      ArrayList<String> expressionStrs = new ArrayList<>();
      operatorObject.get("aggregations:").forEach(exp -> {
        expressionStrs.add(exp.asText());
      });
      expressions = createExpressions(expressionStrs, vertex, sourceExpressionMap, operatorObject);
    }
    else {
      String predicate = operatorObject.get("keys:").textValue();
      expressions = extractExpressions(predicate, vertex, sourceExpressionMap, operatorObject);
    }

    query.getAggregations().addAll(expressions);
  }

  private HashMap<String, Vertex> getVertexIndexHash(Vertex currentVertex, JsonNode inputVertices) {
    HashMap<String, Vertex> vertices = new HashMap<>();

    // Must be a mapper
    if(inputVertices != null) {
      HashMap<String, Vertex> vertexNameMap = new HashMap<>();
      currentVertex.getIncomingVertices().forEach(vertex->{
        vertexNameMap.put(vertex.getName(), vertex);
      });

      inputVertices.fields().forEachRemaining(vertex->{
        vertices.put(vertex.getKey(), vertexNameMap.get(vertex.getValue().textValue()));
      });

      // Guess the current vertex index!
      String currentVertexIndex = vertices.containsKey("0") ? String.valueOf(vertices.size()) : "0";
      vertices.put(currentVertexIndex, currentVertex);
    }
    // Must be a reducer
    else {
      currentVertex.getIncomingVertices().forEach(vertex->{
        vertices.put(String.valueOf(vertices.size()), vertex);
      });
    }

    return vertices;
  }

  // TODO: Reverse of org.apache.hadoop.hive.ql.plan.JoinCondDesc.getJoinCondString in hive side
  // In UTs, handle cases like "Left Outer Join0 to 1", "Inner Join 0 to 1", and swam of keys in Right Join.
  private JoinDefinition getJoinDefinition(String joinCondition) {
    String[] condParts = joinCondition.split("Join");
    String joinType = condParts[0].trim();

    condParts = condParts[1].trim().split(" ");
    String leftIndex = condParts[0].trim();
    String rightIndex = condParts[2].trim();

    return new JoinDefinition(joinType, leftIndex, rightIndex);
  }

  private ArrayList<Expression> extractJoinExprssions(JsonNode operatorObject, String index, HashMap<String, Vertex> verticesHash, Vertex vertex, HashMap<String, Expression> sourceExpressionMap) {
    String columnExp = operatorObject.get("keys:").get(index).textValue();
    Vertex joinedVertex = verticesHash.get(index);
    HashMap<String, Expression> expressionMap = joinedVertex == vertex ? sourceExpressionMap : null;
    return extractExpressions(columnExp, joinedVertex, expressionMap, operatorObject);
  }

  private void extractJoinInfo(String operatorKey, JsonNode operatorObject, Query query, Vertex vertex, HashMap<String, Expression> sourceExpressionMap) {
    final Join.AlgorithmType algorithmType = Join.standardizeAlgType(
        Join.AlgorithmType.vals.get(operatorKey.replace("operator", "").trim())
    );
    HashMap<String, Vertex> verticesHash = vertex.getIncomingVertexIndexHash(operatorObject.get("input vertices:"));

    JsonNode conditionMap = operatorObject.get("condition map:");
    conditionMap.forEach(condition->{
      String joinCondition = condition.get("").textValue();
      JoinDefinition joinDef = getJoinDefinition(joinCondition);

      Join join = new Join(joinDef.getJoinType(), algorithmType);

      // Extract left expression
      join.getLeftExpressions().addAll(extractJoinExprssions(operatorObject, joinDef.getLeftIndex(), verticesHash, vertex, sourceExpressionMap));
      // Extract right expression
      join.getRightExpressions().addAll(extractJoinExprssions(operatorObject, joinDef.getRightIndex(), verticesHash, vertex, sourceExpressionMap));

      if(join.getLeftExpressions().size() == join.getRightExpressions().size()) {
        query.getJoins().add(join);
      }
      else {
        // Throw exception! May be!
      }

    });
  }

  private HashMap<String, Expression> constructNewExpressionMap(JsonNode columnExprMap, Vertex vertex, HashMap<String, Expression> sourceExpressionMap, JsonNode operatorObject) {
    HashMap<String, Expression> expressionMap = new HashMap<>();

    // Create a copy of source expression, and then over-right it of required!
    // Based on the analysis of query plan this is what I could infer!
    expressionMap.putAll(sourceExpressionMap);

    // Overright
    columnExprMap.fields().forEachRemaining(expObj->{
      expressionMap.put(expObj.getKey().toLowerCase(), new Expression(expObj.getValue().asText(), vertex, sourceExpressionMap, operatorObject));
    });

    return expressionMap;
  }

  private void extractOperatorInfo(OperatorType type, String operatorKey, JsonNode operatorObject, Query query, Vertex vertex, HashMap<String, Expression> expressionMap) {
    switch(type) {
      case TABLE_SCAN:
        extractScanInfo(operatorObject, query, vertex);
        break;

      case SELECT:
        extractSelectInfo(operatorObject, query, vertex, expressionMap);
        break;
      case FILTER:
        extractFilterInfo(operatorObject, query, vertex, expressionMap);
        break;
      case GROUP_BY:
        extractGroupByInfo(operatorObject, query, vertex, expressionMap);
        break;

      case JOIN:
        extractJoinInfo(operatorKey, operatorObject, query, vertex, expressionMap);
        break;

      case REDUCE_OUTPUT:
        break;
      case FILE_OUTPUT:
        break;
    }

    if(operatorObject.has("columnExprMap:")) {
      JsonNode columnExprMap = operatorObject.get("columnExprMap:");
      expressionMap = constructNewExpressionMap(columnExprMap, vertex, expressionMap, operatorObject);
    }
    vertex.setExpressionsMap(expressionMap);

    if(operatorObject.has("children")) {
      iterateOperatorTypes(operatorObject.get("children"), query, vertex, expressionMap);
    }
  }

  private void iterateOperatorTypes(JsonNode operatorTypes, Query query, Vertex vertex, HashMap<String, Expression> expressionMap) {
    operatorTypes.fields().forEachRemaining(entity->{
      String operatorKey = entity.getKey().toLowerCase();
      OperatorType operatorType = OperatorType.vals.get(operatorKey);

      if(operatorType == null && operatorKey.endsWith("join operator")) {
        operatorType = OperatorType.JOIN;
      }

      if(operatorType != null) {
        extractOperatorInfo(operatorType, operatorKey, entity.getValue(), query, vertex, expressionMap);
      }
    });
  }

  private HashMap<String, Vertex> createVerticesHash(JsonNode vertices, JsonNode edgesJSON) {
    HashMap<String, Vertex> verticesHash = new HashMap<>();

    // Create vertex instances
    vertices.fields().forEachRemaining(entity->{
      String vertexName = entity.getKey();
      verticesHash.put(vertexName, new Vertex(vertexName, entity.getValue()));
    });

    // Link incoming vertices
    if(edgesJSON != null) {
      for (String vertexName : verticesHash.keySet()) {
        if(edgesJSON.has(vertexName)) {
          JsonNode edgeData = edgesJSON.get(vertexName);
          final ArrayList<JsonNode> edges = new ArrayList<>();

          if(edgeData.isArray()) {
            edgeData.forEach(edge->{
              edges.add(edge);
            });
          }
          else {
            edges.add(edgeData);
          }

          edges.forEach(edge->{
            Vertex incomingVertex = verticesHash.get(edge.get("parent").textValue());
            verticesHash.get(vertexName).getIncomingVertices().add(incomingVertex);
          });
        }
      }
    }

    return verticesHash;
  }

  public Query parse(ObjectNode explainPlan, ArrayNode tablesWritten) {
    JsonNode stagePlans = explainPlan.get("STAGE PLANS");

    Query query = new Query();

    for (JsonNode stagePlan : stagePlans) {
      JsonNode verticesJSON = utils.getJSONObjectFromPath(stagePlan, "Tez", "Vertices:");
      JsonNode edgesJSON = utils.getJSONObjectFromPath(stagePlan, "Tez", "Edges:");

      if(verticesJSON != null) {
        HashMap<String, Vertex> verticesHash = createVerticesHash(verticesJSON, edgesJSON);

        verticesHash.forEach((vertexName, vertex)->{
          JsonNode operatorTree = vertex.getOperatorTree();
          if(operatorTree != null) {
            iterateOperatorTypes(operatorTree, query, vertex, new HashMap<>());
          }
          query.getVertices().add(vertex);
        });
      }
    }

    if(tablesWritten != null) {
      for (JsonNode table : tablesWritten) {
        String tableName = table.get("table").asText();
        String dbName = table.get("database").asText();
        query.getTablesWritten().add(
          new Table(tableName, tableName, dbName, ParsedTableType.NORMAL));
      }
    }

    query.setScans(utils.findUnique(query.getScans()));

    return query;
  }

}
