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
package com.hortonworks.hivestudio.query.parsers;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.StringUtils;
import com.hortonworks.hivestudio.common.orm.EntityField;
import com.hortonworks.hivestudio.common.orm.EntityTable;
import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.query.antlr4.HSBasicSearchLexer;
import com.hortonworks.hivestudio.query.antlr4.HSBasicSearchParser;
import com.hortonworks.hivestudio.query.exceptions.FtsColumnNotFound;
import com.hortonworks.hivestudio.query.parsers.antlr4.BasicSearchVisitorImpl;
import com.hortonworks.hivestudio.query.parsers.listener.ParseErrorListener;

import lombok.extern.slf4j.Slf4j;

/**
 * Query text parser for performing the basic search
 */
@Slf4j
public class BasicSearchParser implements SearchQueryParser {
  private static final String BASE_PREDICATE = "(%s.%s @@ to_tsquery('english', :basicSearchQuery) = true " +
    "OR %s.query_id = :basicSearchText " +
    "OR %s.dag_id = :basicSearchText " +
    "OR %s.application_id = :basicSearchText)";

  private static final String NO_PREDICATE = "true";
  private final EntityTable hiveTable;
  private final EntityTable dagTable;

  public BasicSearchParser(EntityTable hiveTable, EntityTable dagTable) {
    this.hiveTable = hiveTable;
    this.dagTable = dagTable;
  }

  @Override
  public QueryParseResult parse(String queryText) {

    if (StringUtils.isEmpty(queryText)) {
      return new QueryParseResult(NO_PREDICATE, new HashMap<>(), false);
    }
    HSBasicSearchLexer lexer = new HSBasicSearchLexer(CharStreams.fromString(queryText));
    lexer.addErrorListener(ParseErrorListener.INSTANCE);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    HSBasicSearchParser parser = new HSBasicSearchParser(commonTokenStream);
    parser.addErrorListener(ParseErrorListener.INSTANCE);
    HSBasicSearchParser.QueryExpressionContext expressionContext = parser.queryExpression();

    BasicSearchVisitorImpl visitor = new BasicSearchVisitorImpl();
    String tsQueryValue = visitor.visit(expressionContext);

    log.debug("Trigram query generated for query text '{}' is {}", queryText, tsQueryValue);
    Map<String, Object> parametersMap = new HashMap<>();
    parametersMap.put("basicSearchQuery", tsQueryValue);
    parametersMap.put("basicSearchText", queryText);

    Optional<EntityField> queryFtsColumn = hiveTable.getFields().stream().filter(x -> !StringUtils.isEmpty(x.getTsVectorColumnName())).findFirst();

    if (!queryFtsColumn.isPresent()) {
      log.error("Failed to find the FTS column name from the entity class");
      throw new FtsColumnNotFound("Failed to find the FTS column name from the entity class");
    }

    String hiveTablePrefix = hiveTable.getTablePrefix();
    String dagTablePrefix = dagTable.getTablePrefix();

    String predicate = String.format(BASE_PREDICATE,
      hiveTablePrefix, queryFtsColumn.get().getTsVectorColumnName(),
      hiveTablePrefix,
      dagTablePrefix,
      dagTablePrefix
    );

    return new QueryParseResult(predicate, parametersMap, true);
  }
}
