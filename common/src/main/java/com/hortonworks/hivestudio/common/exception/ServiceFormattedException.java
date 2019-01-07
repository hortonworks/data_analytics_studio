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
package com.hortonworks.hivestudio.common.exception;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.Getter;
import lombok.ToString;


/**
 * Generic formatted runtime exception used as a response for APIs
 */
@Getter
@ToString
public class ServiceFormattedException extends RuntimeException implements WebExceptionFormattable {
  private static final int DEFAULT_WEB_RESPONSE_STATUS = 500;
  private String message;
  private String trace;
  private int webResponseStatus;
  private Map<String, Object> otherFields;

  public ServiceFormattedException(String message, Throwable throwable, int webResponseStatus) {
    super(message, throwable);
    this.message = message;
    this.trace = formatTrace(throwable);
    this.webResponseStatus = webResponseStatus;
    this.otherFields = new HashMap<>();
  }

  public ServiceFormattedException(String message, Throwable throwable) {
    this(message, throwable, DEFAULT_WEB_RESPONSE_STATUS);
  }

  public ServiceFormattedException(Throwable throwable, int webResponseStatus) {
    this(null, throwable, webResponseStatus);
  }

  public ServiceFormattedException(Throwable throwable) {
    this(null, throwable, DEFAULT_WEB_RESPONSE_STATUS);
  }

  public ServiceFormattedException(String message, int webResponseStatus) {
    this(message, null, webResponseStatus);
  }

  public ServiceFormattedException(String message) {
    this(message, DEFAULT_WEB_RESPONSE_STATUS);
  }

  protected static String formatTrace(Throwable throwable) {
    if (throwable == null) {
      return null;
    }

    return ExceptionUtils.getStackTrace(throwable);
  }

  public ServiceFormattedException withAdditionalField(String key, Object value) {
    otherFields.put(key, value);
    return this;
  }

  @Override
  public int getWebResponseStatus() {
    return webResponseStatus;
  }
}
