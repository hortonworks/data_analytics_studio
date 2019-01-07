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
package com.hortonworks.hivestudio.common.orm.annotation;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;

import com.hortonworks.hivestudio.common.exception.generic.ConstraintViolationException;
import com.hortonworks.hivestudio.common.orm.EntityField;
import com.hortonworks.hivestudio.common.orm.EntityTable;

import lombok.extern.slf4j.Slf4j;

/**
 * Processes the Annotations on the Entity class and returns the EntityField information for the entity class
 */
@Slf4j
public class EntityFieldProcessor {
  private static final Set<String> prefixesSeen = new HashSet<>();
  private static final String ALPHABETS = "abcdefghijklmnopqrstuvwxyz";
  private static final int[] sizes = {3, 4, 5, 6, 7};

  public static EntityTable process(Class<?> entityClass) {
    List<EntityField> fields = new ArrayList<>();
    String searchQueryPrefix = null;
    String tableName = null;

    if (entityClass.isAnnotationPresent(SearchQuery.class)) {
      SearchQuery sqAnno = entityClass.getAnnotation(SearchQuery.class);
      tableName = sqAnno.table();

      if (!StringUtils.isEmpty(sqAnno.prefix())) {
        if (prefixesSeen.contains(sqAnno.prefix())) {
          log.warn("while processing {}, '{}' prefix already seen before, using a random string " +
              "as entity prefix",entityClass.getName(), sqAnno.prefix());
          searchQueryPrefix = generateRandomString();
        } else {
          searchQueryPrefix = sqAnno.prefix();
        }
      } else {
        searchQueryPrefix = generateRandomString();
      }
      prefixesSeen.add(searchQueryPrefix);

      for (Field field : entityClass.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers())) {
          continue;
        }

        if (!field.isAnnotationPresent(ColumnInfo.class)) {
          continue;
        }

        ColumnInfo columnInfo = field.getAnnotation(ColumnInfo.class);

        boolean isIdField = columnInfo.id();
        boolean isSortable = columnInfo.sortable();
        boolean isSearchable = columnInfo.searchable();
        boolean highlightRequired = columnInfo.highlightRequired();
        boolean isFacetable = columnInfo.facetable();
        boolean isRangeFacetable = columnInfo.rangeFacetable();
        boolean excludeFromFieldsInfo = columnInfo.exclude();
        String tsVectorColumnName = null;
        String highlightProjectionName = "";
        String dbFieldName = columnInfo.columnName();
        String entityFieldName = field.getName();
        String externalFieldName = entityFieldName;
        String displayName = null;

        if (isFacetable && !isSearchable) {
          log.error("'{}' field should be searchable if faceting is required", entityFieldName);
          throw new ConstraintViolationException("'" + entityFieldName + "' field should be searchable if faceting is required");
        }

        if (isFacetable && isRangeFacetable) {
          log.error("'{}' field cannot be facetable and rangefacetable at the same time.", entityFieldName);
          throw new ConstraintViolationException("'" + entityFieldName + "' field cannot be facetable and rangefacetable at the same time.");
        }

        if (!StringUtils.isEmpty(columnInfo.highlightProjectionName())) {
          highlightProjectionName = columnInfo.highlightProjectionName();
        }

        if (!StringUtils.isEmpty(columnInfo.fieldName())) {
          externalFieldName = columnInfo.fieldName();
        }

        if(!StringUtils.isEmpty(columnInfo.tsVectorColumnName())) {
          tsVectorColumnName = columnInfo.tsVectorColumnName();
        }

        displayName = StringUtils.isEmpty(columnInfo.displayName())
            ? getDisplayNameFromFieldName(externalFieldName) : columnInfo.displayName();

        if (displayName == null) {
          displayName = getDisplayNameFromFieldName(entityFieldName);
        }

        EntityField entityField = new EntityField(isIdField, isSearchable, isSortable,
          entityFieldName, dbFieldName, externalFieldName, searchQueryPrefix, highlightRequired,
          highlightProjectionName, tsVectorColumnName, isFacetable, isRangeFacetable, displayName,
          excludeFromFieldsInfo);
        fields.add(entityField);
      }
    } else {
      log.warn("{} is not an entity", entityClass.getName());
    }

    return new EntityTable(tableName, searchQueryPrefix, fields, entityClass);
  }

  private static String getDisplayNameFromFieldName(String fieldName) {
    String capitalize = StringUtils.capitalize(fieldName);
    return capitalize.replaceAll("([A-Z])", " $1").trim();
  }

  private static String generateRandomString() {
    int length = sizes[(int) (sizes.length * Math.random())];
    StringBuilder builder = new StringBuilder();
    IntStream.range(0, length).forEach(x -> {
      builder.append(ALPHABETS.charAt((int) (ALPHABETS.length() * Math.random())));
    });

    String prefix = builder.toString();
    return prefixesSeen.contains(prefix) ? generateRandomString() : prefix;
  }
}
