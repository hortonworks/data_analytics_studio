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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.ParsedColumnType;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

public class Join {

  private static final String INTERMEDIATE_TABLE_PREFIX = "i__";
  private static final String DEFAULT_INTERMEDIATE_TABLE_NAME = "intermediate";
  private static final String DEFAULT_DB_NAME = "default";

  private static final ObjectNode DUMMY_OBJ_NODE = JsonNodeFactory.instance.objectNode();
  private static final Vertex DUMMY_VERTEX = new Vertex("DUMMY", DUMMY_OBJ_NODE);

  public enum Type {
    INNER_JOIN("Inner"),
    FULL_OUTER_JOIN("Outer"),
    LEFT_OUTER_JOIN("Left Outer"),
    RIGHT_OUTER_JOIN("Right Outer"),
    UNIQUE_JOIN("Unique"),
    LEFT_SEMI_JOIN("Left Semi"),
    UNKNOWN("Unknown");

    public static final EnumVals<Join.Type> vals = new EnumVals<>(values());
    private final String value;
    Type(String val) {
      value = val;
    }
    public String toString() {
      return value;
    }
  }

  public enum AlgorithmType {
    JOIN("Join"),
    SHUFFLE_JOIN("Shuffle Join"),

    MAP_JOIN("Map Join"),
    HASH_JOIN("Hash Join"),

    SMB_MAP_JOIN("Sorted Merge Bucket Map Join"),
    SMB_JOIN("SMB Join"),

    MERGE_JOIN("Merge Join"),

    LV_JOIN("Lateral View Join"),

    DP_HASH_JOIN ("Dynamically Partitioned Hash Join");

    public static final EnumVals<Join.AlgorithmType> vals = new EnumVals<>(values());
    private final String value;
    AlgorithmType(String val) {
      value = val;
    }
    public String toString() {
      return value;
    }
  }

  private Type type;
  private AlgorithmType algorithmType;

  private ArrayList<Expression> leftExpressions;
  private ArrayList<Expression> rightExpressions;

  public Type getType() {
    return type;
  }

  public AlgorithmType getAlgorithmType() {
    return algorithmType;
  }

  public ArrayList<Expression> getLeftExpressions() {
    return leftExpressions;
  }

  public ArrayList<Expression> getRightExpressions() {
    return rightExpressions;
  }

  public Join(Type type, AlgorithmType algorithmType) {
    this.type = type;
    this.algorithmType = algorithmType;

    this.leftExpressions = new ArrayList<>();
    this.rightExpressions = new ArrayList<>();
  }

  private ArrayList<JoinLink> getUnique(ArrayList<JoinLink> links) {
    HashMap<String, JoinLink> linkHash = new HashMap<>();

    links.forEach(link -> {
      linkHash.put(link.getExtendedName(), link);
    });

    return new ArrayList<>(linkHash.values());
  }

  private String extractTableNames(ArrayList<Column> columns) {
    ArrayList<String> tables = new ArrayList<>();

    columns.forEach(column -> {
      tables.add(column.getTable().getName());
    });

    return tables.stream().distinct().sorted().collect(Collectors.joining("_"));
  }

  private Column expToColumn(Expression exp, String prefix) {
    String dbName = DEFAULT_DB_NAME;
    String tableName = DEFAULT_INTERMEDIATE_TABLE_NAME;

    ArrayList<Column> columns = exp.getSourceColumns();

    if(!columns.isEmpty()) {
      dbName = columns.get(0).getTable().getDatabaseName();
      tableName = extractTableNames(columns);
    }

    tableName = INTERMEDIATE_TABLE_PREFIX + tableName;
    Table intermediateTable = new Table(tableName, tableName, dbName, ParsedTableType.INTERMEDIATE);

    String columnName = exp.getExpressionString();
    if(!StringUtils.isEmpty(prefix)) {
      columnName = prefix + ":" + columnName;
    }

    return new Column(columnName, intermediateTable);
  }

  private ArrayList<JoinLink> extractSourceExpressionLinks(Expression expression) {
    ArrayList<JoinLink> links = new ArrayList<>();
    Column expColumn = expToColumn(expression, "");
    HashMap<String, Expression> expressionsUsed = expression.getExpressionsUsed();

    if(expressionsUsed.size() != 0) {
      expressionsUsed.forEach((name, exp) -> {
        links.addAll(extractSourceExpressionLinks(exp));
        links.add(new JoinLink(expColumn, expToColumn(exp, name)));
      });
    }
    else {
      expression.getSourceColumns().forEach(column -> {
        links.add(new JoinLink(expColumn, column));
      });
    }

    return links;
  }

  private Expression generateCompositeExpresion(ArrayList<Expression> expressions) {
    HashMap<String, Expression> sourceExpressionMap = new HashMap<>();
    ArrayList<String> expNames = new ArrayList<>();

    for (int i = 0; i < expressions.size(); i++) {
      String name = "exp" + i;
      sourceExpressionMap.put(name, expressions.get(i));
      expNames.add(name);
    }

    String expStr = String.format("(%s)", StringUtils.join(expNames, ", "));
    return new Expression(expStr, DUMMY_VERTEX, sourceExpressionMap, DUMMY_OBJ_NODE);
  }

  public HashSet<Table> extractSourceTables(ArrayList<Column> columns) {

    HashSet<Table> tables = new HashSet<>();

    for (Column column : columns) {
      tables.add(column.getTable());
    }

    return tables;
  }

  private ExpSnippet createOtherSnippet(String expStr, int endIndex, ArrayList<ExpSnippet> snippets) {
    int lastIndex = 0;
    if(snippets.size() > 0) {
      lastIndex = snippets.get(snippets.size() - 1).getEnd();
    }

    String txt = expStr.substring(lastIndex, endIndex);
    return new ExpSnippet(ExpSnippet.Types.OTHERS, txt, lastIndex, endIndex);
  }

  private ArrayList<ExpSnippet> generateSnippets(String expStr, ASTNode node, ArrayList<ExpSnippet> snippets) {

    if(node.getType() == HiveParser.TOK_TABLE_OR_COL) {
      ASTNode childNode = (ASTNode) node.getChild(0);
      CommonToken token = (CommonToken) childNode.getToken();

      snippets.add(createOtherSnippet(expStr, token.getStartIndex(), snippets));

      String txt = expStr.substring(token.getStartIndex(), token.getStopIndex() + 1);
      snippets.add(new ExpSnippet(ExpSnippet.Types.COLUMN, txt, token.getStartIndex(), token.getStopIndex() + 1));
    }

    if(node.getChildren() != null) {
      for (Node child : node.getChildren()) {
        generateSnippets(expStr, (ASTNode) child, snippets);
      }
    }

    return snippets;
  }

  private Column expToColumn(Expression exp) {
    return expToColumn(exp, exp.getSourceColumns());
  }

  private Column expToColumn(Expression exp, ArrayList<Column> columns) {
    Column column = null;
    ASTNode tree = exp.getExpressionTree();

    if(columns.size() == 1) {
      column = columns.get(0);
    }
    else {
      String expStr = Expression.normalizeExpression(exp.getExpressionString());
      ArrayList<ExpSnippet> snippets = generateSnippets(expStr, tree, new ArrayList<>());
      snippets.add(createOtherSnippet(expStr, expStr.length(), snippets));

      HashMap<String, Expression> expUsed = exp.getExpressionsUsed();

      ArrayList<String> tokenTexts = new ArrayList<>();
      for (ExpSnippet snippet : snippets) {
        if(snippet.getType() == ExpSnippet.Types.COLUMN) {
          String expName = Expression.deNormalizeExpression(snippet.getText());
          Expression sourceExpression = expUsed.get(expName);
          Column expAsColumn = expToColumn(sourceExpression);
          tokenTexts.add(expAsColumn.getColumnName());
        }
        else {
          tokenTexts.add(snippet.getText());
        }
      }

      column = new Column(StringUtils.join(tokenTexts, ""), columns.get(0).getTable(), ParsedColumnType.NORMAL);
    }
    return column;
  }

  public JoinLink extractLink() {
    JoinLink link = null;

    if(leftExpressions.size() == rightExpressions.size()) {
      Expression leftExpression, rightExpression;

      if(leftExpressions.size() == 1) {
        leftExpression = leftExpressions.get(0);
        rightExpression = rightExpressions.get(0);
      }
      else {
        leftExpression = generateCompositeExpresion(leftExpressions);
        rightExpression = generateCompositeExpresion(rightExpressions);
      }

      ArrayList<Column> leftSourceColumns = leftExpression.getSourceColumns();
      ArrayList<Column> rightSourceColumns = rightExpression.getSourceColumns();

      if(extractSourceTables(leftSourceColumns).size() == 1 &&
          extractSourceTables(rightSourceColumns).size() == 1) {
        link = new JoinLink(expToColumn(leftExpression, leftSourceColumns), expToColumn(rightExpression, rightSourceColumns));
      }
      else {
        //TODO: Probably log that the join was rejected because of multiple tables
      }
    }
    else {
      //TODO: Throw exception?
    }

    return link;
  }

  public static AlgorithmType standardizeAlgType(AlgorithmType algorithmType) {
    switch (algorithmType) {
      case JOIN:
        algorithmType = AlgorithmType.SHUFFLE_JOIN;
        break;
      case MAP_JOIN:
        algorithmType = AlgorithmType.HASH_JOIN;
        break;
      case SMB_MAP_JOIN:
        algorithmType = AlgorithmType.SMB_JOIN;
        break;
    }

    return algorithmType;
  }
}
