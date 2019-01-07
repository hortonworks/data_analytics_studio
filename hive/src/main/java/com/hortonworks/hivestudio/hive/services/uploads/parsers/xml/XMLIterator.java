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

import com.hortonworks.hivestudio.hive.services.uploads.parsers.EndOfDocumentException;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.RowMapIterator;
import lombok.extern.slf4j.Slf4j;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.util.LinkedHashMap;

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
class XMLIterator implements RowMapIterator {

  private LinkedHashMap<String, String> nextObject = null;
  private static final String TAG_TABLE = "table";
  private static final String TAG_ROW = "row";
  private static final String TAG_COL = "col";
  private boolean documentStarted = false;
  private XMLEventReader reader;

  public XMLIterator(XMLEventReader reader) throws IOException {
    this.reader = reader;
    try {
      nextObject = readNextObject(this.reader);
    } catch (EndOfDocumentException e) {
      log.debug("error : {}", e);
    } catch (XMLStreamException e) {
      throw new IOException(e);
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
    } catch (IOException e) {
      log.error("Exception occured while reading the next row from XML : {} ", e);
      nextObject = null;
    } catch (EndOfDocumentException e) {
      log.debug("End of XML document reached with next character ending the XML.");
      nextObject = null;
    } catch (XMLStreamException e) {
      log.error("Exception occured while reading the next row from XML : {} ", e);
      nextObject = null;
    }
    return currObject;
  }

  @Override
  public void remove() {
    // no operation.
    log.info("No operation when remove called.");
  }

  private LinkedHashMap<String, String> readNextObject(XMLEventReader reader) throws IOException, EndOfDocumentException, XMLStreamException {
    LinkedHashMap<String, String> row = new LinkedHashMap<>();
    boolean objectStarted = false;
    String currentName = null;

    while (true) {
      XMLEvent event = reader.nextEvent();
      switch (event.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          StartElement startElement = event.asStartElement();
          String qName = startElement.getName().getLocalPart();
          log.debug("startName : {}" , qName);
          switch (qName) {
            case TAG_TABLE:
              if (documentStarted) {
                throw new IllegalArgumentException("Cannot have a <table> tag nested inside another <table> tag");
              } else {
                documentStarted = true;
              }
              break;
            case TAG_ROW:
              if (objectStarted) {
                throw new IllegalArgumentException("Cannot have a <row> tag nested inside another <row> tag");
              } else {
                objectStarted = true;
              }
              break;
            case TAG_COL:
              if (!objectStarted) {
                throw new IllegalArgumentException("Stray tag " + qName);
              }
              Attribute nameAttr = startElement.getAttributeByName( new QName("name"));
              if( null == nameAttr ){
                throw new IllegalArgumentException("Missing name attribute in col tag.");
              }
              currentName = nameAttr.getValue();
              break;
            default:
              throw new IllegalArgumentException("Illegal start tag " + qName + " encountered.");
          }
          break;
        case XMLStreamConstants.END_ELEMENT:
          EndElement endElement = event.asEndElement();
          String name = endElement.getName().getLocalPart();
          log.debug("endName : {}", name);
          switch (name) {
            case TAG_TABLE:
              if (!documentStarted) {
                throw new IllegalArgumentException("Stray </table> tag.");
              }
              throw new EndOfDocumentException("End of XML document.");

            case TAG_ROW:
              if (!objectStarted) {
                throw new IllegalArgumentException("Stray </row> tag.");
              }
              return row;

            case TAG_COL:
              if (!objectStarted) {
                throw new IllegalArgumentException("Stray tag " + name);
              }
              currentName = null;
              break;

            default:
              throw new IllegalArgumentException("Illegal start ending " + name + " encountered.");
          }
          break;
        case XMLStreamConstants.CHARACTERS:
          Characters characters = event.asCharacters();
          if (characters.isWhiteSpace() && currentName == null)
            break;
          String data = characters.getData();
          log.debug("character data : {}", data);
          if (currentName == null) {
            throw new IllegalArgumentException("Illegal characters outside any tag : " + data);
          } else {
            String oldData = row.get(currentName);
            if (null != oldData) {
              data = oldData + data;
            }
            row.put(currentName, data);
          }
          break;
      }
    }
  }
}
