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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.hortonworks.hivestudio.common.orm.EntityField;
import com.hortonworks.hivestudio.common.repository.SortRequest.Direction;
import com.hortonworks.hivestudio.query.exceptions.FieldNotSortableException;

/**
 * Sort input parser
 */
public class SortInputParser implements GenericParser<SortParseResult, String> {

  private final Map<String, EntityField> entityFieldMap;
  private final Set<String> allowedSortFields;

  public SortInputParser(List<EntityField> entityFieldList) {
    this.allowedSortFields = extractAllowedSortFields(entityFieldList);
    this.entityFieldMap = extractEntityFieldMap(entityFieldList.stream()
      .filter(x -> allowedSortFields.contains(x.getEntityFieldName()))
      .collect(Collectors.toList())
    );
  }

  private Map<String, EntityField> extractEntityFieldMap(List<EntityField> entityFieldList) {
    return entityFieldList.stream()
      .collect(
        Collectors.toMap(EntityField::getExternalFieldName, y -> y)
      );
  }

  private Set<String> extractAllowedSortFields(List<EntityField> entityFieldList) {
    return entityFieldList.stream()
      .filter(EntityField::isSortable)
      .map(EntityField::getExternalFieldName)
      .collect(Collectors.toSet());
  }

  @Override
  public SortParseResult parse(String parseInput) {
    if (StringUtils.isEmpty(parseInput)) {
      return SortParseResult.empty();
    }

    String[] inputs = parseInput.split(",");
    boolean sortingRequired = inputs.length > 0;
    String sortExpression = extractSortingString(inputs);

    return new SortParseResult(sortingRequired, sortExpression);
  }

  private String extractSortingString(String[] inputs) {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (String input : inputs) {
      String[] parts = input.trim().split(":");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid sort field: " + input);
      }
      Direction direction = Direction.valueOf(parts[1].toUpperCase());
      if (!allowedSortFields.contains(parts[0])) {
        throw new FieldNotSortableException("Field not sortable: " + parts[0]);
      }
      if (!first) {
        builder.append(", ");
      }
      EntityField e = entityFieldMap.get(parts[0]);
      builder.append(e.getEntityPrefix());
      builder.append('.');
      builder.append(e.getDbFieldName());
      builder.append(' ');
      builder.append(direction);
      first = false;
    }
    return builder.toString();
  }
}
