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
package com.hortonworks.hivestudio.query.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.jdbi.v3.core.mapper.JoinRow;

import com.hortonworks.hivestudio.common.dto.FacetEntry;
import com.hortonworks.hivestudio.common.dto.FacetValue;
import com.hortonworks.hivestudio.common.dto.HiveQueryDto;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.exception.generic.ConstraintViolationException;
import com.hortonworks.hivestudio.common.orm.EntityField;
import com.hortonworks.hivestudio.common.repository.PageData;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.query.dto.FieldInformation;
import com.hortonworks.hivestudio.query.dto.SearchRequest;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;
import com.hortonworks.hivestudio.query.exceptions.SearchTypeException;
import com.hortonworks.hivestudio.query.generators.queries.CountQueryGenerator;
import com.hortonworks.hivestudio.query.generators.queries.FacetQueryGenerator;
import com.hortonworks.hivestudio.query.generators.queries.HighlightQueryFunctionGenerator;
import com.hortonworks.hivestudio.query.generators.queries.NullHighlightQueryFunctionGenerator;
import com.hortonworks.hivestudio.query.generators.queries.SearchQueryGenerator;
import com.hortonworks.hivestudio.query.parsers.FacetInputParser;
import com.hortonworks.hivestudio.query.parsers.FacetParseResult;
import com.hortonworks.hivestudio.query.parsers.QueryParseResult;
import com.hortonworks.hivestudio.query.parsers.RangeFacetInputParser;
import com.hortonworks.hivestudio.query.parsers.SearchQueryParser;
import com.hortonworks.hivestudio.query.parsers.SortInputParser;
import com.hortonworks.hivestudio.query.parsers.SortParseResult;
import com.hortonworks.hivestudio.query.parsers.TimeRangeInputParser;
import com.hortonworks.hivestudio.query.parsers.TimeRangeParseResult;

import jersey.repackaged.com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

/**
 * Service to perform the search
 */
@Slf4j
public class SearchService {
  private static final int MAX_FACET_FIELDS_SEARCH_ALLOWED = 3;
  private static final int DEFAULT_SEARCH_DURATION = 7 * 24 * 60 * 60 * 1000;
  private static final String RANGE_FACET_IDENTIFIER_TEXT = "range-facet";
  private static final String FACET_IDENTIFIER_TEXT = "facet";

  private static List<FieldInformation> fieldsInformation = null;
  private static final int MAX_LIMIT = 100;
  private static final String SORT_FRAGMENT = "ORDER BY %s";

  private final SearchQueryParser basicParser;
  private final SortInputParser sortInputParser;
  private final FacetInputParser facetParser;
  private final RangeFacetInputParser rangeFacetParser;
  private final TimeRangeInputParser timeRangeInputParser;
  private final Provider<HiveQueryRepository> hiveQueryRepoProvider;

  @Inject
  public SearchService(@Named("Basic") SearchQueryParser basicParser,
                       SortInputParser sortInputParser,
                       FacetInputParser facetParser,
                       RangeFacetInputParser rangeFacetParser,
                       TimeRangeInputParser timeRangeInputParser,
                       Provider<HiveQueryRepository> hiveQueryRepoProvider) {
    this.basicParser = basicParser;
    this.sortInputParser = sortInputParser;
    this.facetParser = facetParser;
    this.rangeFacetParser = rangeFacetParser;
    this.timeRangeInputParser = timeRangeInputParser;
    this.hiveQueryRepoProvider = hiveQueryRepoProvider;
  }

  public PageData<HiveQueryDto> doBasicSearch(String queryText, String sortText, Integer offset,
      Integer limit, Long startTime, Long endTime, List<SearchRequest.Facet> facets,
      List<SearchRequest.RangeFacet> rangeFacets, String username) {
    HiveQueryRepository repository = hiveQueryRepoProvider.get();
    int iOffset = sanitizeOffset(offset);
    int iLimit = sanitizeLimit(limit);
    String iQueryText = sanitizeQuery(queryText);
    String iSortText = sanitizeQuery(sortText);
    Long iEndTime = sanitizeEndTime(startTime, endTime);
    Long iStartTime = sanitizeStartTime(startTime, endTime);

    QueryParseResult parseResult = basicParser.parse(iQueryText);
    SortParseResult sortParseResult = sortInputParser.parse(iSortText);
    FacetParseResult facetParseResult = facetParser.parse(facets);
    FacetParseResult rangeFacetParseResult = rangeFacetParser.parse(rangeFacets);
    TimeRangeParseResult timeRangeParseResult = timeRangeInputParser.parse(new Pair<>(iStartTime, iEndTime));

    String sortFragment = sortParseResult.isSortingRequired()
        ? String.format(SORT_FRAGMENT, sortParseResult.getSortExpression()) : "";

    Optional<EntityField> highlightField = Stream.concat(
        HiveQuery.TABLE_INFORMATION.getFields().stream(),
        DagInfo.TABLE_INFORMATION.getFields().stream())
            .filter(EntityField::isHighlightRequired)
            .findFirst();

    final String highlightQueryFunction;
    if (parseResult.isQueryHighLightRequired() && highlightField.isPresent()) {
      highlightQueryFunction = new HighlightQueryFunctionGenerator("basicSearchQuery").generate(highlightField.get());
    } else {
      highlightQueryFunction = new NullHighlightQueryFunctionGenerator().generate(EntityField.dummyWithProjection("highlighted_query"));
    }

    List<String> predicates = Lists.newArrayList(parseResult.getPredicate(),
        facetParseResult.getFacetExpression(), rangeFacetParseResult.getFacetExpression(),
        timeRangeParseResult.getTimeRangeExpression());
    if (username != null) {
      predicates.add(HiveQuery.TABLE_INFORMATION.getTablePrefix() + ".request_user = :username");
    }

    SearchQueryGenerator searchQueryGenerator = new SearchQueryGenerator(
        HiveQuery.TABLE_INFORMATION, DagInfo.TABLE_INFORMATION, "offset", "limit",
        highlightQueryFunction, sortFragment, predicates);
    String finalSql = searchQueryGenerator.generate();

    CountQueryGenerator countQueryGenerator = new CountQueryGenerator(
        HiveQuery.TABLE_INFORMATION, DagInfo.TABLE_INFORMATION, "query_count", predicates);
    String finalCountSql = countQueryGenerator.generate();

    Map<String, Object> parameterBindings = new HashMap<>();
    parameterBindings.putAll(parseResult.getParameterBindings());
    parameterBindings.putAll(facetParseResult.getParameterBindings());
    parameterBindings.putAll(rangeFacetParseResult.getParameterBindings());
    parameterBindings.putAll(timeRangeParseResult.getParameterBindings());
    if (username != null) {
      parameterBindings.put("username", username);
    }

    Map<String, Object> countParameterBindings = new HashMap<>(parameterBindings);

    parameterBindings.put("limit", iLimit);
    parameterBindings.put("offset", iOffset);

    log.debug("Final query: {}", finalSql);
    log.debug("Final count query: {}", finalCountSql);

    List<JoinRow> rows = repository.executeSearchQuery(finalSql, parameterBindings);
    List<HiveQueryDto> queries = new ArrayList<>(rows.size());
    for (JoinRow row : rows) {
      queries.add(new HiveQueryDto(row.get(HiveQuery.class), null, row.get(DagInfo.class)));
    }
    Long queryCount = repository.executeSearchCountQuery(finalCountSql, countParameterBindings);

    return new PageData<>(queries, iOffset, iLimit, queryCount);
  }

  public List<HiveQuery> doAdvancedSearch(String queryText, Integer offset, Integer limit) {
    offset = sanitizeOffset(offset);
    limit = sanitizeLimit(limit);
    // TODO: Perform Advanced Search
    return null;
  }

  @DASTransaction
  public Pair<List<FacetValue>, List<FacetValue>> getFacetValues(String queryText,
      String facetFieldsText, Long startTime, Long endTime, String username) {
    HiveQueryRepository repository = hiveQueryRepoProvider.get();
    String iQueryText = sanitizeQuery(queryText);
    Set<String> facetFields = extractFacetFields(facetFieldsText);
    Long iEndTime = sanitizeEndTime(startTime, endTime);
    Long iStartTime = sanitizeStartTime(startTime, endTime);

    if (facetFields.size() > MAX_FACET_FIELDS_SEARCH_ALLOWED) {
      log.error("Max allowed facets to be queries is {}. Current queried fields is {}",
          MAX_FACET_FIELDS_SEARCH_ALLOWED, facetFields.size());
      throw new ConstraintViolationException("Max allowed facets to be queries is " +
          MAX_FACET_FIELDS_SEARCH_ALLOWED + ". Current queried fields is " + facetFields.size(),
          Response.Status.BAD_REQUEST.getStatusCode());
    }

    QueryParseResult parseResult = basicParser.parse(iQueryText);
    TimeRangeParseResult timeRangeParseResult = timeRangeInputParser.parse(new Pair<>(iStartTime, iEndTime));

    List<String> predicates = Lists.newArrayList(parseResult.getPredicate(),
        timeRangeParseResult.getTimeRangeExpression());
    if (username != null) {
      predicates.add(HiveQuery.TABLE_INFORMATION.getTablePrefix() + ".request_user = :username");
    }

    FacetQueryGenerator facetCountQuery = new FacetQueryGenerator(HiveQuery.TABLE_INFORMATION,
        DagInfo.TABLE_INFORMATION, new HashSet<>(facetFields), predicates);
    String finalSql = facetCountQuery.generate();
    log.debug("Final facet query: {}", finalSql);

    Map<String, Object> parameterBindings = new HashMap<>();
    parameterBindings.putAll(parseResult.getParameterBindings());
    parameterBindings.putAll(timeRangeParseResult.getParameterBindings());
    parameterBindings.put("username", username);

    List<FacetEntry> entries = repository.executeFacetQuery(finalSql, parameterBindings);
    return extractFacetsFromEntries(entries);
  }

  private Pair<List<FacetValue>, List<FacetValue>> extractFacetsFromEntries(List<FacetEntry> entries) {
    ArrayList<FacetEntry> entriesArrayList = new ArrayList<>(entries);
    Stream<Integer> indexStream = IntStream.range(0, entries.size()).boxed().filter(i -> {
      FacetEntry facetEntry = entriesArrayList.get(i);
      return facetEntry.isFirst();
    });

    List<Integer> partitionIndexes = indexStream.collect(Collectors.toList());
    partitionIndexes.add(entries.size());

    List<List<FacetEntry>> subSets = IntStream.range(0, partitionIndexes.size() - 1)
        .mapToObj(i -> entriesArrayList.subList(partitionIndexes.get(i), partitionIndexes.get(i + 1)))
        .collect(Collectors.toList());

    List<FacetValue> facets = new ArrayList<>();
    List<FacetValue> rangeFacets = new ArrayList<>();

    for(List<FacetEntry> subSet : subSets) {
      FacetEntry topEntry = subSet.get(0);
      String facetKey = topEntry.getType().toLowerCase();
      List<FacetEntry> facetEntries = subSet.subList(1, subSet.size());
      if(facetKey.startsWith(RANGE_FACET_IDENTIFIER_TEXT)) {
        if (rangeFacetShouldBeAdded(facetEntries)) {
          rangeFacets.add(new FacetValue(topEntry.getKey(), facetEntries));
        }
      }

      if(facetKey.startsWith(FACET_IDENTIFIER_TEXT)) {
        facets.add(new FacetValue(topEntry.getKey(), facetEntries));
      }
    }
    return new Pair<>(facets, rangeFacets);
  }

  private boolean rangeFacetShouldBeAdded(List<FacetEntry> facetEntries) {
    if(facetEntries.size() != 2) return false;
    long nullValueEntryCount = facetEntries.stream().filter(x -> x.getValue() == null).count();
    return nullValueEntryCount == 0;
  }

  private Set<String> extractFacetFields(String facetFieldsText) {
    return Arrays.stream(facetFieldsText.split(",")).map(String::trim).collect(Collectors.toSet());
  }


  private String sanitizeQuery(String queryText) {
    return queryText == null ? "" : queryText;
  }

  private int sanitizeOffset(Integer offset) {
    return offset == null ? 0 : offset;
  }

  private Long sanitizeStartTime(Long startTime, Long endTime) {
    if (startTime == null && endTime == null) {
      return System.currentTimeMillis() - DEFAULT_SEARCH_DURATION;
    } else if (startTime == null){
      return endTime - DEFAULT_SEARCH_DURATION;
    } else {
      return startTime;
    }
  }

  private Long sanitizeEndTime(Long startTime, Long endTime) {
    if (startTime == null && endTime == null) {
      return System.currentTimeMillis();
    } else if (endTime == null){
      return startTime + DEFAULT_SEARCH_DURATION;
    } else {
      return endTime;
    }
  }

  private int sanitizeLimit(Integer limit) {
    return limit == null ? MAX_LIMIT : limit > MAX_LIMIT ? MAX_LIMIT : limit;
  }

  public static List<FieldInformation> getFieldsInformation() {
    if (fieldsInformation == null) {
      synchronized (SearchService.class) {
        Function<EntityField, FieldInformation> entityFieldConsumer = x -> new FieldInformation(
          x.getExternalFieldName(), x.getDisplayName(), x.isSearchable(), x.isSortable(), x.isFacetable(), x.isRangeFacetable());
        fieldsInformation = new ArrayList<>();

        fieldsInformation.addAll(HiveQuery.TABLE_INFORMATION.getFields().stream()
          .filter(x -> !x.isExclude())
          .map(entityFieldConsumer)
          .collect(Collectors.toList()));
        fieldsInformation.addAll(DagInfo.TABLE_INFORMATION.getFields().stream()
          .filter(x -> !x.isExclude())
          .map(entityFieldConsumer)
          .collect(Collectors.toList()));
      }
    }
    return fieldsInformation;
  }

  /**
   * Defines the different search types allowed
   */
  public enum SearchType {
    BASIC,
    ADVANCED;

    /**
     * Throws runtime exception if the searchType text doesnot match any SearchType
     *
     * @return SearchType for the string
     */
    public static SearchType validateAndGetSearchType(String searchType) {
      if (StringUtils.isEmpty(searchType)) {
        throw new SearchTypeException("Search type is required and cannot be empty");
      }
      try {
        return SearchType.valueOf(searchType.toUpperCase());
      } catch (IllegalArgumentException ex) {
        throw new SearchTypeException("Search type '" + searchType + "' not allowed", ex);
      }
    }
  }
}
