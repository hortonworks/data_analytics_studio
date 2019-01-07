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
package com.hortonworks.hivestudio.eventProcessor.configuration;

import java.util.HashMap;

public class EventProcessingConfig extends HashMap<String, Object> {
  private static final long serialVersionUID = -2171082432652253501L;

  public Object get(String key, Object value){
    Object val = get(key);
    if(null == val){
      return value;
    }else {
      return val;
    }
  }

  public String getAsString(String key, Object value){
    Object val = get(key, value);
    if(null == val){
      return null;
    }else{
      return val.toString();
    }
  }

  public Long getAsLong(String key, Object value){
    Object val = get(key, value);
    if( val == null )
      return null;

    if( val instanceof Long){
      return (Long) val;
    }else if( val instanceof Number){
      return ((Number) val).longValue();
    }else {
      return Long.parseLong(val.toString());
    }
  }

  public Boolean getAsBoolean(String key, Boolean value){
    Object val = get(key, value);
    if( val == null )
      return null;

    if( val instanceof Boolean){
      return (Boolean) val;
    }else {
      return Boolean.parseBoolean(val.toString());
    }
  }

  public Integer getAsInteger(String key, Object value){
    Object val = get(key, value);
    if( val == null )
      return null;

    if( val instanceof Integer){
      return (Integer) val;
    }else if( val instanceof Number){
      return ((Number) val).intValue();
    }else {
      return Integer.parseInt(val.toString());
    }
  }
}
