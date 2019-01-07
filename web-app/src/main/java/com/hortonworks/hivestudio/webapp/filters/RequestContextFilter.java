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
package com.hortonworks.hivestudio.webapp.filters;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.resource.RequestContext;

import lombok.extern.slf4j.Slf4j;

/**
 * Jersey filter to extract the context information from the request and create the context object.
 */
@Provider
@Slf4j
public class RequestContextFilter implements ContainerRequestFilter {
  public static final String REQUEST_CONTEXT_PROPERTY_NAME = "REQUEST-CONTEXT";

  @Context
  private HttpServletRequest servletRequest;

  @Context
  private AppAuthentication appAuth;

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    String username = null;

    HttpSession session = servletRequest.getSession(false);
    if (session != null) {
      username = (String)session.getAttribute(Constants.SESSION_USER_KEY);
    }

    if (username == null) {
      username = appAuth.getAppUser();
    }

    if (username == null) {
      log.error("Username not configured, assuming hive user, things might break badly");
      username = "hive";
    }

    log.debug("Final username: {}", username);
    String connectionUrl = (String) session.getAttribute(Constants.CONNECTION_URL_KEY);
    RequestContext context = new RequestContext(1L, username, appAuth.getRole(username), connectionUrl);
    containerRequestContext.setProperty(REQUEST_CONTEXT_PROPERTY_NAME, context);
  }
}
