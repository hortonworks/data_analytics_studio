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


package com.hortonworks.hivestudio.hive.internal.parsers;


import com.hortonworks.hivestudio.hive.client.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class AbstractTableMetaParser<T> implements TableMetaSectionParser<T> {
  private final String sectionMarker;
  private final String secondarySectionMarker;
  private final String sectionStartMarker;
  private final String sectionEndMarker;


  public AbstractTableMetaParser(String sectionMarker, String sectionStartMarker, String sectionEndMarker) {
    this(sectionMarker, null, sectionStartMarker, sectionEndMarker);
  }

  public AbstractTableMetaParser(String sectionMarker, String secondarySectionMarker, String sectionStartMarker, String sectionEndMarker) {
    this.sectionMarker = sectionMarker;
    this.secondarySectionMarker = secondarySectionMarker;
    this.sectionStartMarker = sectionStartMarker;
    this.sectionEndMarker = sectionEndMarker;
  }

  protected Map<String, Object> parseSection(List<Row> rows) {
    boolean sectionStarted = false;
    boolean startMarkerAndEndMarkerIsSame = !(sectionStartMarker == null || sectionEndMarker == null) && sectionStartMarker.equalsIgnoreCase(sectionEndMarker);
    boolean sectionDataReached = false;

    Map<String, Object> result = new LinkedHashMap<>();

    Iterator<Row> iterator = rows.iterator();

    String currentNestedEntryParent = null;
    List<Entry> currentNestedEntries = null;
    boolean processingNestedEntry = false;

    while (iterator.hasNext()) {
      Row row = iterator.next();
      String colName = ((String) row.getRow()[0]).trim();
      String colValue = row.getRow()[1] != null ? ((String) row.getRow()[1]).trim() : null;
      String colComment = row.getRow()[2] != null ? ((String) row.getRow()[2]).trim() : null;

      if (sectionMarker.equalsIgnoreCase(colName)) {
        sectionStarted = true;
      } else {
        if (sectionStarted) {
          if (secondarySectionMarker != null && secondarySectionMarker.equalsIgnoreCase(colName) && colValue != null) {
            continue;
          }

          if (sectionStartMarker != null && sectionStartMarker.equalsIgnoreCase(colName) && colValue == null) {
            if (startMarkerAndEndMarkerIsSame) {
              if (sectionDataReached) {
                break;
              }
            }
            sectionDataReached = true;
            continue;
          } else if (sectionEndMarker != null && sectionEndMarker.equalsIgnoreCase(colName) && colValue == null) {
            break;
          } else if (sectionStartMarker == null) {
            sectionDataReached = true;
            //continue;
          }

          if (colValue == null && !processingNestedEntry) {
            currentNestedEntryParent = colName;
            currentNestedEntries = new ArrayList<>();
            processingNestedEntry = true;
            continue;
          } else if (colName.equalsIgnoreCase("") && processingNestedEntry) {
            Entry entry = new Entry(colValue, colComment);
            currentNestedEntries.add(entry);
            continue;
          } else if (processingNestedEntry) {
            result.put(currentNestedEntryParent, currentNestedEntries);
            processingNestedEntry = false;
          }

          Entry entry = new Entry(colName, colValue, colComment);
          result.put(colName, entry);

        }

      }
    }

    if (processingNestedEntry) {
      result.put(currentNestedEntryParent, currentNestedEntries);
    }

    return result;
  }

  protected Map<String, String> getMap(Map<String, Object> parsedSection, String key) {
    Map<String, String> result = new HashMap<>();
    Object value = parsedSection.get(key);
    if(value == null) {
      return null;
    }
    if (value instanceof List) {
      List<Entry> entries = (List<Entry>)value;
      for(Entry entry: entries) {
        result.put(entry.getName(), entry.getValue());
      }
    }
    return result;
  }

  protected String getString(Map<String, Object> parsedSection, String key) {
    Object value = parsedSection.get(key);
    if(value == null) {
      return null;
    }
    if (value instanceof Entry) {
      return ((Entry) parsedSection.get(key)).getValue();
    }
    return null;
  }


  public static class Entry {
    private final String name;
    private final String value;
    private final String comment;

    public Entry(String name, String type, String comment) {
      this.name = name;
      this.value = type;
      this.comment = comment;
    }

    public Entry(String name, String type) {
      this(name, type, null);
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    public String getComment() {
      return comment;
    }
  }
}
