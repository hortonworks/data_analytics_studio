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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.hortonworks.hivestudio.common.orm.EntityField;
import com.hortonworks.hivestudio.query.dto.FieldInformation;
import com.hortonworks.hivestudio.query.dto.SearchRequest;

public class FacetInputParser implements GenericParser<FacetParseResult, List<SearchRequest.Facet>> {

  private final Set<String> facetableFieldNames;
  private final Map<String, EntityField> entityFieldMap;

  private static int facetCount = 0;

  public FacetInputParser(List<EntityField> entityFieldList, List<FieldInformation> fields) {
    facetableFieldNames = fields.stream()
      .filter(FieldInformation::isFacetable)
      .map(FieldInformation::getFieldName)
      .collect(Collectors.toSet());

    this.entityFieldMap = extractEntityFieldMap(entityFieldList.stream()
      .filter(x -> facetableFieldNames.contains(x.getEntityFieldName()))
      .collect(Collectors.toList())
    );
  }

  private Map<String, EntityField> extractEntityFieldMap(List<EntityField> entityFieldList) {
    return entityFieldList.stream()
      .collect(
        Collectors.toMap(EntityField::getExternalFieldName, y -> y)
      );
  }


  @Override
  public FacetParseResult parse(List<SearchRequest.Facet> facets) {
    Map<String, Object> parameterBindings = new HashMap<>();
    if (facets == null || facets.size() == 0)
      return FacetParseResult.empty();

    Set<SearchRequest.Facet> filteredFacets = facets.stream()
      .filter(x -> facetableFieldNames.contains(x.getField()))
      .collect(Collectors.toSet());

    Set<String> facetBindParameters = filteredFacets.stream().map(x -> {
      EntityField entityField = entityFieldMap.get(x.getField());

      if (entityField.isTableReadOrWrittenField()) {
        String jsonFragment = getFacetFragmentAfterJsonProcessing(x, parameterBindings);
        String dbColumnName = entityField.getEntityPrefix() + "." + entityField.getDbFieldName();
        return dbColumnName + " @> ANY ( ARRAY["  + jsonFragment + "]::jsonb[])";
      } else {
        String fragment = getFacetFragmentAfterNormalProcessing(parameterBindings, x);
        String dbColumnName = entityField.getEntityPrefix() + "." + entityField.getDbFieldName();
        return dbColumnName + " IN (" + fragment + ")";
      }
    }).collect(Collectors.toSet());

    // If empty values list was provided
    if(parameterBindings.size() == 0) return FacetParseResult.empty();


    String expression = String.join(" AND ", facetBindParameters);
    return new FacetParseResult(expression, parameterBindings);
  }

  private String getFacetFragmentAfterNormalProcessing(Map<String, Object> parameterBindings, SearchRequest.Facet x) {
    List<String> bindParameters = x.getValues().stream().map(y -> {
      String bindParameterName = getNextBindParameterName();
      parameterBindings.put(bindParameterName, y);
      return ":" + bindParameterName;
    }).collect(Collectors.toList());
    return String.join(", ", bindParameters);
  }

  private String getFacetFragmentAfterJsonProcessing(SearchRequest.Facet x, Map<String, Object> parameterBindings) {
    // Hack to mitigate the issue that JPA does not process bind parameters within single quotes
    List<String> parameters = x.getValues().stream().map(y -> {
      String value = (String) y;
      String[] splits = value.split("\\.");
      String table = splits[1];
      String database = splits[0];
      return "[{\"table\": \"" + table + "\", \"database\": \"" + database + "\"}]";
    }).collect(Collectors.toList());

    return parameters.stream().map(y -> {
      String paramName = getNextBindParameterName();
      parameterBindings.put(paramName, y);
      return ":" + paramName;
    }).collect(Collectors.joining(", "));
  }

  private String getNextBindParameterName() {
    facetCount++;
    if (facetCount > 100000) facetCount = 0;
    return "facet" + facetCount;
  }
}
