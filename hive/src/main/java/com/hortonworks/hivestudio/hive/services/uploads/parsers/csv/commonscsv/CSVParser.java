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
package com.hortonworks.hivestudio.hive.services.uploads.parsers.csv.commonscsv;

import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.ParseOptions;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.Parser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * Parses the given Reader which contains CSV stream and extracts headers and rows, and detect datatypes of columns
 */
@Slf4j
public class CSVParser extends Parser {
  private CSVIterator iterator;
  private org.apache.commons.csv.CSVParser parser;

  public CSVParser(Reader reader, ParseOptions parseOptions) throws IOException {
    super(reader, parseOptions);
    CSVFormat format = CSVFormat.DEFAULT;
    String optHeader =  (String)parseOptions.getOption(ParseOptions.OPTIONS_HEADER);
    if(optHeader != null){
      if(optHeader.equals(ParseOptions.HEADER.FIRST_RECORD.toString())) {
        format = format.withHeader();
      }else if( optHeader.equals(ParseOptions.HEADER.PROVIDED_BY_USER.toString())){
        String [] headers = (String[]) parseOptions.getOption(ParseOptions.OPTIONS_HEADERS);
        format = format.withHeader(headers);
      }
    }

    Character delimiter = (Character) parseOptions.getOption(ParseOptions.OPTIONS_CSV_DELIMITER);
    if(delimiter != null){
      log.info("setting delimiter as {}", delimiter);
      format = format.withDelimiter(delimiter);
    }

    Character quote = (Character) parseOptions.getOption(ParseOptions.OPTIONS_CSV_QUOTE);
    if( null != quote ){
      log.info("setting Quote char : {}", quote);
      format = format.withQuote(quote);
    }

    Character escape = (Character) parseOptions.getOption(ParseOptions.OPTIONS_CSV_ESCAPE_CHAR);
    if(escape != null){
      log.info("setting escape as {}", escape);
      format = format.withEscape(escape);
    }

    parser = new org.apache.commons.csv.CSVParser(this.reader,format );
    iterator = new CSVIterator(parser.iterator());
  }

  @Override
  public Row extractHeader() {
    return new Row(parser.getHeaderMap().keySet().toArray());
  }

  @Override
  public void close() throws Exception {
    this.parser.close();
  }

  public Iterator<Row> iterator() {
    return iterator; // only one iterator per parser.
  }
}
