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
import com.hortonworks.hivestudio.hive.exceptions.ServiceException;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.csv.opencsv.OpenCSVParser;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.json.JSONParser;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.xml.XMLParser;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * Wrapper/Decorator over the Stream parsers.
 * Supports XML/JSON/CSV parsing.
 */
public class DataParser implements IParser {

  private IParser parser;

  public DataParser(Reader reader, ParseOptions parseOptions) throws IOException {
    if (parseOptions.getOption(ParseOptions.OPTIONS_FILE_TYPE).equals(ParseOptions.InputFileType.CSV.toString())) {
      parser = new OpenCSVParser(reader, parseOptions);
    } else if (parseOptions.getOption(ParseOptions.OPTIONS_FILE_TYPE).equals(ParseOptions.InputFileType.JSON.toString())) {
      parser = new JSONParser(reader, parseOptions);
    } else if (parseOptions.getOption(ParseOptions.OPTIONS_FILE_TYPE).equals(ParseOptions.InputFileType.XML.toString())) {
      parser = new XMLParser(reader, parseOptions);
    }
  }

  @Override
  public PreviewData parsePreview() {
    return parser.parsePreview();
  }

  @Override
  public Row extractHeader() {
    return parser.extractHeader();
  }

  @Override
  public void close() throws Exception {
    parser.close();
  }

  @Override
  public Iterator<Row> iterator() {
    return parser.iterator();
  }
}
