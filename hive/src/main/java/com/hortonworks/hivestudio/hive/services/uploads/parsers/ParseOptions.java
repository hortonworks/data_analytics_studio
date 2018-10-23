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

import java.util.HashMap;

public class ParseOptions {
  public static final String OPTIONS_CSV_DELIMITER = "OPTIONS_CSV_DELIMITER";
  public static final String OPTIONS_CSV_QUOTE = "OPTIONS_CSV_QUOTE";
  public static final String OPTIONS_HEADERS = "OPTIONS_HEADERS";
  public static final String OPTIONS_CSV_ESCAPE_CHAR = "OPTIONS_CSV_ESCAPE_CHAR";

  public enum InputFileType {
    CSV,
    JSON,
    XML
  }

  public enum HEADER {
    FIRST_RECORD,
    PROVIDED_BY_USER, // not used right now but can be used when some metadata of file provide this information
    EMBEDDED, // this one is for JSON/ XML and may be other file formats where its embedded with the data
    NONE   // if the file does not contain header information at all
  }
  final public static String OPTIONS_FILE_TYPE = "FILE_TYPE";
  final public static String OPTIONS_HEADER = "HEADER";
  final public static String OPTIONS_NUMBER_OF_PREVIEW_ROWS = "NUMBER_OF_PREVIEW_ROWS";

  private HashMap<String, Object> options = new HashMap<>();

  public void setOption(String key, Object value) {
    this.options.put(key, value);
  }

  public Object getOption(String key) {
    return this.options.get(key);
  }

  @Override
  public String toString() {
    return new StringBuilder("ParseOptions{")
      .append("options=").append(options)
      .append('}').toString();
  }
}
