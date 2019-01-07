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
import com.google.gson.stream.JsonToken;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.EndOfDocumentException;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.RowMapIterator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * iterates over the JsonReader and reads creates row data
 * assumes the array of json objects.
 * eg : [ { "col1Name" : "value-1-1", "col2Name" : "value-1-2"}, { "col1Name" : "value-2-1", "col2Name" : "value-2-2"}]
 */
@Slf4j
class JSONIterator implements RowMapIterator {

  private LinkedHashMap<String, String> nextObject = null;

  private LinkedHashMap<String, String> readNextObject(JsonReader reader) throws IOException, EndOfDocumentException {
    LinkedHashMap<String, String> row = new LinkedHashMap<>();
    boolean objectStarted = false;
    boolean shouldBeName = false;
    String currentName = null;

    while (true) {
      JsonToken token = reader.peek();
      switch (token) {
        case BEGIN_ARRAY:
          throw new IllegalArgumentException("Row data cannot have an array.");
        case END_ARRAY:
          throw new EndOfDocumentException("End of Json Array document.");
        case BEGIN_OBJECT:
          if (objectStarted == true) {
            throw new IllegalArgumentException("Nested objects not supported.");
          }
          if (shouldBeName == true) {
            throw new IllegalArgumentException("name expected, got begin_object");
          }
          objectStarted = true;
          shouldBeName = true;
          reader.beginObject();
          break;
        case END_OBJECT:
          if (shouldBeName == false) {
            throw new IllegalArgumentException("value expected, got end_object");
          }
          reader.endObject();
          return row;
        case NAME:
          if (shouldBeName == false) {
            throw new IllegalArgumentException("name not expected at this point.");
          }
          shouldBeName = false;
          currentName = reader.nextName();
          break;
        case NUMBER:
        case STRING:
          if (shouldBeName == true) {
            throw new IllegalArgumentException("value not expected at this point.");
          }
          String n = reader.nextString();
          row.put(currentName, n);
          shouldBeName = true;
          break;
        case BOOLEAN:
          if (shouldBeName == true) {
            throw new IllegalArgumentException("value not expected at this point.");
          }
          String b = String.valueOf(reader.nextBoolean());
          row.put(currentName, b);
          shouldBeName = true;
          break;
        case NULL:
          if (shouldBeName == true) {
            throw new IllegalArgumentException("value not expected at this point.");
          }
          reader.nextNull();
          row.put(currentName, "");
          shouldBeName = true;
          break;
        case END_DOCUMENT:
          return row;

        default:
          throw new IllegalArgumentException("Illegal token detected inside json: token : " + token.toString());
      }
    }
  }

  private JsonReader reader;

  public JSONIterator(JsonReader reader) throws IOException {
    this.reader = reader;
    // test the start of array
    JsonToken jt = reader.peek();
    if (jt != JsonToken.BEGIN_ARRAY) {
      throw new IllegalArgumentException("Expected the whole document to contain a single JsonArray.");
    }

    reader.beginArray(); // read the start of array
    try {
      nextObject = readNextObject(this.reader);
    } catch (EndOfDocumentException e) {
    }
  }

  @Override
  public boolean hasNext() {
    return null != nextObject;
  }

  public LinkedHashMap<String, String> peek() {
    return nextObject;
  }

  @Override
  public LinkedHashMap<String, String> next() {
    LinkedHashMap<String, String> currObject = nextObject;
    try {
      nextObject = readNextObject(this.reader);
    } catch (EndOfDocumentException e) {
      log.debug("End of Json document reached with next character ending the JSON Array.");
      nextObject = null;
    } catch (Exception e){
      // for any other exception throw error right away
      throw new IllegalArgumentException(e);
    }
    return currObject;
  }

  @Override
  public void remove() {
    // no operation.
    log.info("No operation when remove called on JSONIterator.");
  }
}