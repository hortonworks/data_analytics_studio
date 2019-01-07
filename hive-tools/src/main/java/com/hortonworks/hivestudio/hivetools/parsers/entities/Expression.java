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
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class Expression {

  private static ParseDriver parseDriver = new ParseDriver();

  private String incomingVertexIndex;
  private String expressionString;
  private List<String> columnNames;
  private Vertex currentVertex;

  // Map of expressions used to create the current expression!
  private HashMap<String, Expression> sourceExpressionMap;
  // Map of vertices whose values are used to create the current expression!
  private JsonNode operatorObject;

  public String getExpressionString() {
    return expressionString;
  }

  public Expression(String expressionString, Vertex vertex, HashMap<String, Expression> sourceExpressionMap, JsonNode operatorObject) {
    expressionString = expressionString.trim();

    // TODO: Must get this data as an object from Hive side
    this.incomingVertexIndex = extractSourceVertexIndex(expressionString);
    this.expressionString = this.incomingVertexIndex != null ? expressionString.substring(incomingVertexIndex.length() + 1) : expressionString;

    this.columnNames = extractColumnNames();

    this.currentVertex = vertex;
    this.sourceExpressionMap = sourceExpressionMap;
    this.operatorObject = operatorObject;
  }

  // TODO: BUG-92583: Remove following 2 hack functions once parseExpression is made better or a better parser is available!
  public static String normalizeExpression(String expression) {
    expression = expression.replaceAll("KEY.", "KEY_");
    expression = expression.replaceAll("VALUE.", "VALUE_");
    expression = expression.replaceAll("_col", "DEFAULT_col");
    return expression;
  }

  public static String deNormalizeExpression(String expression) {
    expression = expression.replaceAll("DEFAULT_col", "_col");
    expression = expression.replaceAll("VALUE_", "VALUE.");
    expression = expression.replaceAll("KEY_", "KEY.");
    return expression;
  }

  public ASTNode getExpressionTree() {
    ASTNode node = null;

    String normalizedExpression = normalizeExpression(expressionString);

    try {
      node = parseDriver.parseExpression(normalizedExpression);
    } catch (Exception e) {
      log.error("Parsing failed on expression {}, error: {}",
          normalizedExpression, e.getMessage());
    }

    return node;
  }

  private String extractSourceVertexIndex(String expression) {
    int colonIndex = expression.indexOf(":");
    if(colonIndex != -1) {
      return expression.substring(0, colonIndex);
    }
    return null;
  }

  private ArrayList<String> getNamesFromAST(ASTNode node) {
    ArrayList<String> names = new ArrayList<>();

    if(node.getType() == HiveParser.TOK_TABLE_OR_COL) {
      names.add(deNormalizeExpression(node.getChild(0).toString()));
    }

    ArrayList<Node> childNodes = node.getChildren();
    if(childNodes != null) {
      for(Node childNode : node.getChildren()) {
        names.addAll(getNamesFromAST((ASTNode) childNode));
      }
    }

    return names;
  }

  public HashMap<String, Expression> getExpressionMap() {
    return sourceExpressionMap != null ? sourceExpressionMap : currentVertex.getExpressionsMap();
  }

  public HashMap<String, Expression> getExpressionsUsed() {
    HashMap<String, Expression> expressions = new HashMap<>();
    HashMap<String, Expression> expressionMap = getExpressionMap();

    List<String> columnNames = extractColumnNames();

    if (columnNames != null) {

      columnNames.forEach(name -> {
        Boolean expressionsAdded = false;

        // Check in incoming vertex if index is available
        if (incomingVertexIndex != null) {
          HashMap<String, Vertex> vertexHash = currentVertex.getIncomingVertexIndexHash(operatorObject.get("input vertices:"));
          Vertex sourceVertex = vertexHash.get(incomingVertexIndex);
          if (sourceVertex != currentVertex) {
            expressionsAdded = true;
            expressions.put(name, sourceVertex.getExpressionsMap().get(name));
          }
        }

        if (!expressionsAdded) {
          // Else check source expressions in current vertex
          if (expressionMap.containsKey(name)) {
            expressions.put(name, expressionMap.get(name));
          }
          // Else check incoming vertices
          else {
            currentVertex.getIncomingVertices().forEach(incomingVertex -> {
              if (incomingVertex.getExpressionsMap().containsKey(name)) {
                expressions.put(name, incomingVertex.getExpressionsMap().get(name));
              }
            });
          }
        }
      });
    }

    return expressions;
  }

  public List<String> extractColumnNames() {
    List<String> names = null;

    ASTNode node = getExpressionTree();

    if (node != null) {
      names = getNamesFromAST(node);
    }

    if(null != names){
      names = names.stream().map(String::toLowerCase).collect(Collectors.toList());
    }

    return names;
  }

  // TODO: This is a costly function, uses of this must be limited/optimised
  public ArrayList<Column> getSourceColumns() {
    ArrayList<Column> columns = new ArrayList<>();

    HashMap<String, Expression> expressionsUsed = getExpressionsUsed();
    if (expressionsUsed.size() > 0) {
      expressionsUsed.forEach((name, exp) -> {
        columns.addAll(exp.getSourceColumns());
      });
    } else if (columnNames != null) {
      columnNames.forEach(name -> {
        // Else check current vertex column
        if (currentVertex.getColumnsScanned().containsKey(name)) {
          columns.add(currentVertex.getColumnsScanned().get(name));
        }
      });
    }

    return columns;
  }

  public ArrayList<Column> getSourceColumns(String columnName) {
    HashMap<String, Expression> expressionsUsed = getExpressionsUsed(); // We have scope for optimization here

    ArrayList<Column> columns = new ArrayList<>();

    if (expressionsUsed.containsKey(columnName)) {
      columns.addAll(expressionsUsed.get(columnName).getSourceColumns());
    } else if (currentVertex.getColumnsScanned().containsKey(columnName)) {
      columns.add(currentVertex.getColumnsScanned().get(columnName));
    }

    return columns;
  }
}