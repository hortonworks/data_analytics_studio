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


package com.hortonworks.hivestudio.hive.client;


import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Wrapper over other iterables. Does not block and can be reset to start again from beginning.
 */
public class PersistentCursor<T, R> implements Cursor<T, R>  {
  private List<T> rows = Lists.newArrayList();
  private List<R> columns = Lists.newArrayList();
  private int offset = 0;

  public PersistentCursor(List<T> rows, List<R> columns) {
    this.rows = rows;
    this.columns = columns;
  }


  @Override
  public Iterator<T> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return rows.size() > 0 && offset < rows.size();
  }

  @Override
  public T next() {
    T row = rows.get(offset);
    offset++;
    return row;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Read only cursor. Method not supported");
  }

  @Override
  public boolean isResettable() {
    return true;
  }

  @Override
  public void reset() {
    this.offset = 0;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public List<R> getDescriptions() {
    return columns;
  }

  @Override
  public void keepAlive() {
    // Do Nothing as we are pre-fetching everything.
  }
}
