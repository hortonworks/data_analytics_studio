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

import com.google.common.base.Strings;
import com.hortonworks.hivestudio.hive.client.ColumnDescription.DataTypes;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Slf4j
public class ParseUtils {

  final public static DataTypes[] dataTypeList = {DataTypes.BOOLEAN, DataTypes.INT, DataTypes.BIGINT, DataTypes.DOUBLE, DataTypes.CHAR, DataTypes.TIMESTAMP, DataTypes.DATE, DataTypes.STRING};
  private static final String HIVE_DATE_FORMAT = "yyyy-MM-dd";

  // no strict checking required as it is done by Date parsing
  private static final String HIVE_DATE_FORMAT_REGEX = "^[0-9]{4}-[0-9]?[0-9]-[0-9]?[0-9]$";


  public static boolean isInteger(Object object) {
    if (object == null)
      return false;

    if (object instanceof Integer)
      return true;

    try {
      Integer i = Integer.parseInt(object.toString());
      return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }

  public static boolean isBoolean(Object object) {
    if (object == null)
      return false;

    if (object instanceof Boolean)
      return true;

    String strValue = object.toString();
    return strValue.equalsIgnoreCase("true") || strValue.equalsIgnoreCase("false");
  }

  public static boolean isString(Object object) {
    return object != null;
  }

  public static boolean isLong(Object object) {
    if (object == null)
      return false;

    if (object instanceof Long)
      return true;

    try {
      Long i = Long.parseLong(object.toString());
      return true;
    } catch (Exception nfe) {
      return false;
    }
  }

  public static boolean isDouble(Object object) {
    if (object == null)
      return false;

    if (object instanceof Double)
      return true;

    try {
      Double i = Double.parseDouble(object.toString());
      return true;
    } catch (Exception nfe) {
      return false;
    }
  }

  public static boolean isChar(Object object) {
    if (object == null)
      return false;

    if (object instanceof Character)
      return true;

    String str = object.toString().trim();
    return str.length() == 1;

  }

  public static boolean isDate(Object object) {
    if (object == null)
      return false;

    if (object instanceof Date)
      return true;

    String str = object.toString();
    if (Strings.isNullOrEmpty(str)) {
      str = str.trim();
      if (str.matches(HIVE_DATE_FORMAT_REGEX)) {
        try {
          SimpleDateFormat sdf = new SimpleDateFormat(HIVE_DATE_FORMAT);
          sdf.setLenient(false);
          Date date = sdf.parse(str);
          return true;
        } catch (Exception e) {
          log.debug("error while parsing as date string {}, format {}", str, HIVE_DATE_FORMAT, e);
        }
      }
    }
    return false;
  }

  public static boolean isTimeStamp(Object object) {
    if (object == null)
      return false;

    if (object instanceof Date)
      return true;

    String str = object.toString();
    try {
      Timestamp ts = Timestamp.valueOf(str);
      return true;
    } catch (Exception e) {
      log.debug("error while parsing as timestamp string {}", str, e);
    }

    return false;
  }

  public static DataTypes detectHiveDataType(Object object) {
    // detect Integer
    if (isBoolean(object)) return DataTypes.BOOLEAN;
    if (isInteger(object)) return DataTypes.INT;
    if (isLong(object)) return DataTypes.BIGINT;
    if (isDouble(object)) return DataTypes.DOUBLE;
    if (isChar(object)) return DataTypes.CHAR;
    if (isTimeStamp(object)) return DataTypes.TIMESTAMP;
    if (isDate(object)) return DataTypes.DATE;

    return DataTypes.STRING;
  }

  public static boolean checkDatatype( Object object, DataTypes datatype){
    switch(datatype){

      case BOOLEAN :
        return isBoolean(object);
      case INT :
        return isInteger(object);
      case BIGINT :
        return isLong(object);
      case DOUBLE:
        return isDouble(object);
      case CHAR:
        return isChar(object);
      case DATE:
        return isDate(object);
      case TIMESTAMP:
        return isTimeStamp(object);
      case STRING:
        return isString(object);

      default:
        log.error("this datatype detection is not supported : {}", datatype);
        return false;
    }
  }

  public static DataTypes detectHiveColumnDataType(List<Object> colValues) {
    boolean found;
    for(DataTypes datatype : dataTypeList){
      found = true;
      for(Object object : colValues){
        if(!checkDatatype(object,datatype)){
          found = false;
          break;
        }
      }

      if(found) return datatype;
    }

    return DataTypes.STRING; //default
  }
}
