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
package com.hortonworks.hivestudio.hive.services.uploads.parsers.csv.opencsv;

import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.ParseOptions;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.Parser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * Parses the given Reader which contains CSV stream and extracts headers and rows
 */
@Slf4j
public class OpenCSVParser extends Parser {
  private Row headerRow;
  private OpenCSVIterator iterator;
  private CSVReader csvReader = null;

  public OpenCSVParser(Reader reader, ParseOptions parseOptions) throws IOException {
    super(reader, parseOptions);
    CSVParserBuilder csvParserBuilder = new CSVParserBuilder();
    CSVReaderBuilder builder =  new CSVReaderBuilder(reader);

    Character delimiter = (Character) parseOptions.getOption(ParseOptions.OPTIONS_CSV_DELIMITER);
    if(delimiter != null){
      log.info("setting delimiter as {}", delimiter);
      csvParserBuilder = csvParserBuilder.withSeparator(delimiter);
    }

    Character quote = (Character) parseOptions.getOption(ParseOptions.OPTIONS_CSV_QUOTE);
    if( null != quote ){
      log.info("setting Quote char : {}", quote);
      csvParserBuilder = csvParserBuilder.withQuoteChar(quote);
    }

    Character escapeChar = (Character) parseOptions.getOption(ParseOptions.OPTIONS_CSV_ESCAPE_CHAR);
    if( null != escapeChar ){
      log.info("setting escapeChar : {}", escapeChar);
      csvParserBuilder = csvParserBuilder.withEscapeChar(escapeChar);
    }

    builder.withCSVParser(csvParserBuilder.build());
    this.csvReader = builder.build();
    iterator = new OpenCSVIterator(this.csvReader.iterator());

    String optHeader =  (String)parseOptions.getOption(ParseOptions.OPTIONS_HEADER);
    if(optHeader != null){
      if(optHeader.equals(ParseOptions.HEADER.FIRST_RECORD.toString())) {
        this.headerRow = iterator().hasNext() ? iterator.next() : new Row(new Object[]{});
      }
    }

  }

  @Override
  public Row extractHeader() {
    return headerRow;
  }

  @Override
  public void close() throws Exception {
    this.csvReader.close();
  }

  public Iterator<Row> iterator() {
    return iterator; // only one iterator per parser.
  }
}
