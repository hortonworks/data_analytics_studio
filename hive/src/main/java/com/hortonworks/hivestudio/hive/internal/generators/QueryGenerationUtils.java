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
package com.hortonworks.hivestudio.hive.internal.generators;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;

public class QueryGenerationUtils {

  public static final String ADDED = "ADDED";
  public static final String DELETED = "DELETED";
  public static final String MODIFIED = "MODIFIED";

  public static <K, V> boolean isNullOrEmpty(Map<K, V> map) {
    return null != map && !map.isEmpty();
  }

  public static <T> boolean isNullOrEmpty(Collection<T> collection) {
    return null == collection || collection.isEmpty();
  }

  public static <K, V> boolean isEqual(Map<K, V> oldProps, Map<K, V> newProps) {
    if (oldProps == newProps) {
      return true;
    }
    if (oldProps == null || newProps == null) {
      return false;
    }
    if (oldProps.size() != newProps.size()) {
      return false;
    }
    for (Map.Entry<K, V> e : oldProps.entrySet()) {
      if (!Objects.equals(e.getValue(), newProps.get(e.getKey()))) {
        return false;
      }
    }
    return true;
  }

  /**
   * return a map with 3 keys "DELETED" and "ADDED" and "MODIFIED" to show the different between oldProps and newProps
   * for "ADDED" and "MODIFIED" the values in map are of newProps
   * @param oldProps
   * @param newProps
   * @return
   */
  public static <K, V> Optional<Map<String, Map<K, V>>> findDiff(Map<K, V> oldProps, Map<K, V> newProps) {
    Map<K, V> added = new HashMap<>();
    Map<K, V> modified = new HashMap<>();
    Map<K, V> deleted = new HashMap<>();

    if (oldProps == null && newProps == null) {
      return Optional.empty();
    }

    if (oldProps == null) {
      oldProps = new HashMap<>();
    }
    if (newProps == null) {
      newProps = new HashMap<>();
    }

    for (Map.Entry<K, V> e : oldProps.entrySet()) {
      K key = e.getKey();
      V oldValue = e.getValue();
      V newValue = newProps.get(key);
      if (!Objects.equals(oldValue, newValue)) {
        if (oldValue == null) {
          added.put(key, newValue);
        } else if (newValue == null) {
          deleted.put(key, oldValue);
        } else {
          modified.put(key, newValue);
        }
      }
    }
    for (Map.Entry<K, V> e : newProps.entrySet()) {
      if (e.getValue() != null && oldProps.get(e.getKey()) == null) {
        added.put(e.getKey(), e.getValue());
      }
    }

    Map<String, Map<K, V>> ret = new HashMap<>();
    ret.put(ADDED, added);
    ret.put(DELETED, deleted);
    ret.put(MODIFIED, modified);
    return Optional.of(ret);
  }

  public static String getPropertiesAsKeyValues(Map<String, String> parameters) {
    List<String> props = FluentIterable.from(parameters.entrySet())
            .transform(new Function<Map.Entry<String, String>, String>() {
              @Nullable
              @Override
              public String apply(@Nullable Map.Entry<String, String> entry) {
                return "'" + entry.getKey() + "'='" + entry.getValue() + "'";
              }
            }).toList();

    return Joiner.on(",").join(props);
  }

  public static String getColumnRepresentation(ColumnInfo column) {
    StringBuilder colQuery = new StringBuilder().append("`").append(column.getName()).append("`");
    colQuery.append(" ").append(column.getType());
    if(!QueryGenerationUtils.isNullOrZero(column.getPrecision())){
      if(!QueryGenerationUtils.isNullOrZero(column.getScale())){
        colQuery.append("(").append(column.getPrecision()).append(",").append(column.getScale()).append(")");
      }else{
        colQuery.append("(").append(column.getPrecision()).append(")");
      }
    }
    if(!Strings.isNullOrEmpty(column.getComment())) {
      colQuery.append(" COMMENT '").append(column.getComment()).append("'");
    }

    return colQuery.toString();
  }

  public static boolean isNullOrZero(Integer integer) {
    return null == integer || 0 == integer;
  }
}
