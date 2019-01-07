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
package com.hortonworks.hivestudio.hive.services.uploads.parsers.json;

import com.google.gson.stream.JsonReader;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.ParseOptions;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.Parser;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.RowIterator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Iterator;


/**
 * Parses the input data from reader as JSON and provides iterator for rows.
 *
 * Expects the input reader to contains a JsonArray in which each element is a JsonObject
 * corresponding to the row.
 * eg. :
 *
 * [
 *  {row1-col1, row1-col2, row1-col3},
 *  {row2-col1, row2-col2, row2-col3}
 * ]
 *
 */
@Slf4j
public class JSONParser extends Parser {

  private RowIterator iterator;
  private JsonReader jsonReader;
  private JSONIterator JSONIterator;

  public JSONParser(Reader reader, ParseOptions parseOptions) throws IOException {
    super(reader, parseOptions);
    this.jsonReader = new JsonReader(this.reader);
    JSONIterator = new JSONIterator(this.jsonReader);
    iterator = new RowIterator(JSONIterator);
  }

  @Override
  public Row extractHeader() {
    Collection<String> headers = this.iterator.extractHeaders();
    Object[] objs = new Object[headers.size()];
    Iterator<String> iterator = headers.iterator();
    for(int i = 0 ; i < headers.size() ; i++){
      objs[i] = iterator.next();
    }

    return new Row(objs);
  }

  @Override
  public void close() throws Exception {
    this.jsonReader.close();
  }

  @Override
  public Iterator<Row> iterator() {
    return iterator;
  }
}