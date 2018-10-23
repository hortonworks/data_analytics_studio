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
package com.hortonworks.hivestudio.hive.services.uploads.parsers.xml;

import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.ParseOptions;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.Parser;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.RowIterator;
import lombok.extern.slf4j.Slf4j;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Iterator;

/**
 * assumes XML of following format
 * <table>
 * <row>
 * <col name="col1Name">row1-col1-Data</col>
 * <col name="col2Name">row1-col2-Data</col>
 * <col name="col3Name">row1-col3-Data</col>
 * <col name="col4Name">row1-col4-Data</col>
 * </row>
 * <row>
 * <col name="col1Name">row2-col1-Data</col>
 * <col name="col2Name">row2-col2-Data</col>
 * <col name="col3Name">row2-col3-Data</col>
 * <col name="col4Name">row2-col4-Data</col>
 * </row>
 * </table>
 */
@Slf4j
public class XMLParser extends Parser {

  private RowIterator iterator;
  private XMLEventReader xmlReader;
  private XMLIterator xmlIterator;

  public XMLParser(Reader reader, ParseOptions parseOptions) throws IOException {
    super(reader, parseOptions);
    XMLInputFactory factory = XMLInputFactory.newInstance();
    try {
      factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
      factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
      this.xmlReader = factory.createXMLEventReader(reader);
    } catch (XMLStreamException e) {
      log.error("error occurred while creating xml reader : ", e);
      throw new IOException("error occurred while creating xml reader : ", e);
    }
    xmlIterator = new XMLIterator(this.xmlReader);
    iterator = new RowIterator(xmlIterator);
  }

  @Override
  public Row extractHeader() {
    Collection<String> headers = this.iterator.extractHeaders();
    Object[] objs = new Object[headers.size()];
    Iterator<String> iterator = headers.iterator();
    for (int i = 0; i < headers.size(); i++) {
      objs[i] = iterator.next();
    }

    return new Row(objs);
  }

  @Override
  public void close() throws Exception {
    try {
      this.xmlReader.close();
    } catch (XMLStreamException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return iterator;
  }
}
