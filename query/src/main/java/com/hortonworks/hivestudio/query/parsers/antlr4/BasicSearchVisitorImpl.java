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
package com.hortonworks.hivestudio.query.parsers.antlr4;


import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.StringUtils;
import com.hortonworks.hivestudio.query.antlr4.HSBasicSearchBaseVisitor;
import com.hortonworks.hivestudio.query.antlr4.HSBasicSearchParser;

/**
 * Antlr parser implementation for the Basic search
 */
public class BasicSearchVisitorImpl extends HSBasicSearchBaseVisitor<String> {

  private static final String DQ = "\"";
  private static final String SQ = "'";

  @Override
  public String visitQueryExpression(HSBasicSearchParser.QueryExpressionContext ctx) {
    List<String> tokens = ctx.children.stream()
      .map(tree -> {
        if (tree instanceof HSBasicSearchParser.UqStringContext) {
          return visitUqString((HSBasicSearchParser.UqStringContext) tree);
        } else if (tree instanceof HSBasicSearchParser.SqStringContext) {
          return visitSqString((HSBasicSearchParser.SqStringContext) tree);
        } else if (tree instanceof HSBasicSearchParser.DqStringContext) {
          return visitDqString((HSBasicSearchParser.DqStringContext) tree);
        } else {
          return "";
        }
      }).filter(x -> !StringUtils.isEmpty(x))
      .collect(Collectors.toList());


    return "'" + String.join(" & ", tokens) + "'";
  }

  @Override
  public String visitUqString(HSBasicSearchParser.UqStringContext ctx) {
    return ctx.STRING().getText();
  }

  @Override
  public String visitDqString(HSBasicSearchParser.DqStringContext ctx) {
    List<String> tokens = extractTokensFromQuotedString(ctx, DQ);
    return "''" + String.join("", tokens) +  "''";
  }

  @Override
  public String visitSqString(HSBasicSearchParser.SqStringContext ctx) {
    List<String> tokens = extractTokensFromQuotedString(ctx, SQ);
    return "''" + String.join("", tokens) +  "''";
  }

  private List<String> extractTokensFromQuotedString(ParserRuleContext ctx, String terminalGuideToken) {
    return ctx.children.stream()
      .map(tree -> {
        if(tree instanceof HSBasicSearchParser.WhitespaceContext) {
          return visitWhitespace((HSBasicSearchParser.WhitespaceContext) tree);
        } else {
          if (!tree.getText().equalsIgnoreCase(terminalGuideToken)) {
            return tree.getText();
          } else {
            return "";
          }
        }
      })
      .filter(x -> !StringUtils.isEmpty(x))
      .collect(Collectors.toList());
  }

  @Override
  public String visitWhitespace(HSBasicSearchParser.WhitespaceContext ctx) {
    return ctx.getText();
  }
}
