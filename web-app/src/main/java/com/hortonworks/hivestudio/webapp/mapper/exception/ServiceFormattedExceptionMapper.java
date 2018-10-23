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
package com.hortonworks.hivestudio.webapp.mapper.exception;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;

/**
 * Exception mapper for ServiceFormattedException
 */

@Provider
public class ServiceFormattedExceptionMapper implements ExceptionMapper<ServiceFormattedException> {
  @Override
  public Response toResponse(ServiceFormattedException exception) {
    return toEntity(exception.getMessage(), exception.getTrace(), exception.getWebResponseStatus(), exception.getOtherFields());
  }

  private Response toEntity(String message, String trace, int status, Map<String, Object> otherFields) {
    Map<String, Object> errorMap = getErrorMap(message, trace, status, otherFields);

    Map<String, Map<String, Object>> responseMap = new HashMap<>();
    responseMap.put("error", errorMap);

    return Response.status(status).entity(responseMap).type(MediaType.APPLICATION_JSON).build();
  }

  private Map<String, Object> getErrorMap(String message, String trace, int status, Map<String, Object> otherFields) {
    Map<String, Object> errorMap = new HashMap<>();
    if (!StringUtils.isEmpty(message)) {
      errorMap.put("message", message);
    }

    if (!StringUtils.isEmpty(trace)) {
      errorMap.put("trace", trace);
    }

    errorMap.put("status", status);

    if (otherFields != null) {
      otherFields.forEach(errorMap::put);
    }

    return errorMap;
  }

}
