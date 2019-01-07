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

import com.hortonworks.hivestudio.common.config.AuthConfig;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.PrintWriter;

import static com.hortonworks.hivestudio.common.Constants.SESSION_USER_KEY;

@Slf4j
public class LdapFilter implements Filter {
  private AuthConfig authConfig;

  @Inject
  public LdapFilter(AuthConfig authConfig){
    this.authConfig = authConfig;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
    log.debug("In Ldap sso filter ... {}", httpRequest.getRequestURI());
    HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;

    //      if this is assets, let it go.
    String pathInfo = ((HttpServletRequest) servletRequest).getPathInfo();
    log.info("pathInfo = {}", pathInfo);
    if (pathInfo.startsWith("/assets")) {
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }

      HttpSession session = httpRequest.getSession();

    // No sso configured or user is already authenticated.
    if (!authConfig.isLdapSSOEnabled() || session.getAttribute(SESSION_USER_KEY) != null) {
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    } else {
//      if this is login call, let it go.
      if (pathInfo.equals("/login")) {
        filterChain.doFilter(servletRequest, servletResponse);
        return;
      } else {
        // if username is not available then send authentication error
        sendUnauthorizeError(httpRequest, httpServletResponse);
        return;
      }
    }
  }

  private void sendUnauthorizeError(HttpServletRequest httpRequest, HttpServletResponse httpServletResponse) throws ServletException, IOException {
    PrintWriter writer = httpServletResponse.getWriter();
    httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    httpServletResponse.setHeader("Content-Type", "application/json");
    writer.write("{\"errors\":{\"message\":\"Ldap password required\"}}");
    writer.flush();
    writer.close();
  }

  @Override
  public void destroy() {

  }
}
