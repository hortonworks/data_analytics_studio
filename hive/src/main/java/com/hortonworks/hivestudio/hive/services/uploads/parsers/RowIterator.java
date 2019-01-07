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
package com.hortonworks.hivestudio.hive.services.uploads.parsers;

import com.hortonworks.hivestudio.hive.client.Row;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * Converts the Map of values created by JSON/XML Parser into ordered values in Row
 * Takes RowMapIterator as input
 */
public class RowIterator implements Iterator<Row> {

  private LinkedList<String> headers = null;
  private RowMapIterator iterator;

  /**
   * creates a row iterator for the map values in RowMapIterator
   * keeps the keys in map as header.
   * @param iterator
   */
  public RowIterator(RowMapIterator iterator) {
    this.iterator = iterator;
    LinkedHashMap<String, String> obj = iterator.peek();
    headers = new LinkedList<>();
    if (null != obj) {
      headers.addAll(obj.keySet());
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }


  @Override
  public Row next() {
    LinkedHashMap<String, String> r = this.iterator.next();
    if (null == r) {
      return null;
    }

    return convertToRow(r);
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  /**
   * @return : ordered collection of string of headers
   */
  public LinkedList<String> extractHeaders() {
    return headers;
  }

  /**
   * converts the map into a Row
   * @param lr
   * @return
   */
  private Row convertToRow(LinkedHashMap<String, String> lr) {
    Object[] data = new Object[headers.size()];
    int i = 0;
    for (String cd : headers) {
      String d = lr.get(cd);

      if (d != null)
        d = d.trim(); // trim to remove any \n etc which is used as a separator for rows in TableDataReader

      data[i++] = d;
    }

    return new Row(data);
  }

}