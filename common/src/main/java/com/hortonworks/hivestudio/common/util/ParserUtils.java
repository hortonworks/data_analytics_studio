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


package com.hortonworks.hivestudio.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserUtils {

  public static final String DATA_TYPE_REGEX = "\\s*([^() ]+)\\s*(\\s*\\(\\s*([0-9]+)\\s*(\\s*,\\s*([0-9]+))?\\s*\\)\\s*)?\\s*";

  /**
   * @param columnDataTypeString : the string that needs to be parsed as a datatype example : decimal(10,3)
   * @return a list of string containing type, precision and scale in that order if present, null otherwise
   */
  public static List<String> parseColumnDataType(String columnDataTypeString) {
    List<String> typePrecisionScale = new ArrayList<>(3);

    Pattern pattern = Pattern.compile(DATA_TYPE_REGEX);
    Matcher matcher = pattern.matcher(columnDataTypeString);

    if (matcher.find()) {
      typePrecisionScale.add(matcher.group(1));
      typePrecisionScale.add(matcher.group(3));
      typePrecisionScale.add(matcher.group(5));
    }else{
      typePrecisionScale.add(null);
      typePrecisionScale.add(null);
      typePrecisionScale.add(null);
    }

    return typePrecisionScale;
  }
}
