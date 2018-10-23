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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Date;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.http.client.utils.URIBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.AuthConfig;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KnoxSSOFilter implements Filter {
  private static final String AJAX_REDIRECT_PATH = "/";
  public static final String SESSION_USER_KEY = "username";
  private static final int SESSION_VALIDITY_TIME = 24 * 60 * 60; // 1 day.
  private static final ObjectMapper mapper = new ObjectMapper();

  private final AuthConfig authConfig;
  private final String[] webUserAgents;
  private JWSVerifier verifier;

  @Inject
  public KnoxSSOFilter(AuthConfig authConfig) {
    this.authConfig = authConfig;
    this.webUserAgents = authConfig.getKnoxUserAgent().toLowerCase().split(",");
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    if (authConfig.isKnoxSSOEnabled()) {
      this.verifier = new RSASSAVerifier(toRSAKey(authConfig.getKnoxPublicKey()));
    }
  }

  @Override
  public void destroy() {
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
      FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
    log.debug("In knox sso filter ... {}", httpRequest.getRequestURI());
    HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
    HttpSession session = httpRequest.getSession();

    // No sso configured or user is already authenticated.
    if (!authConfig.isKnoxSSOEnabled() || session.getAttribute(SESSION_USER_KEY) != null) {
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }

    String username = extractJWTUser(httpRequest);
    if (username != null) {
      log.debug("Knox SSO user: {}", username);
      synchronized (session) {
        if (session.getAttribute(SESSION_USER_KEY) == null) {
          session.setAttribute(SESSION_USER_KEY, username);
          session.setMaxInactiveInterval(SESSION_VALIDITY_TIME);
        }
      }
      filterChain.doFilter(httpRequest, httpServletResponse);
    } else if (isWebUserAgent(httpRequest.getHeader("User-Agent"))) {
      // if the jwt token is not available then redirect it to knox sso
      redirectToKnox(httpRequest, httpServletResponse);
    } else {
      httpServletResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
    }
  }

  private String extractJWTUser(HttpServletRequest request) {
    String serializedJWT = getJWTCookie(request);
    if (serializedJWT != null) {
      try {
        SignedJWT jwtToken = SignedJWT.parse(serializedJWT);
        if (validateToken(jwtToken)) {
          String username = jwtToken.getJWTClaimsSet().getSubject();
          return (username == null || username.length() == 0) ? null : username;
        }
      } catch (ParseException e) {
        log.warn("Invalid jwt token: {}", serializedJWT, e);
      }
    }
    return null;
  }

  private String getJWTCookie(HttpServletRequest request) {
    String cookieName = authConfig.getKnoxCookieName();
    Cookie[] cookies = request.getCookies();
    if (cookieName != null && cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          return cookie.getValue();
        }
      }
    }
    return null;
  }

  private boolean validateToken(SignedJWT jwtToken) {
    if (JWSObject.State.SIGNED == jwtToken.getState() && jwtToken.getSignature() != null) {
      try {
        if (jwtToken.verify(verifier)) {
          Date expires = jwtToken.getJWTClaimsSet().getExpirationTime();
          return (expires == null || new Date().before(expires));
        }
      } catch (ParseException | JOSEException e) {
        log.warn("Error while validating jwtToken", e);
      }
    }
    return false;
  }

  private boolean isWebUserAgent(String userAgent) {
    if (userAgent != null) {
      userAgent = userAgent.toLowerCase();
      for (String webUA : webUserAgents) {
        if (userAgent.contains(webUA)) {
          return true;
        }
      }
    }
    return false;
  }

  private void redirectToKnox(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    if (request.getHeader(Constants.XSRF_HEADER) != null) {
      // Ajax request send redirect url in json
      String ajaxReturnUrl = request.getScheme() + "://" + request.getServerName() + ":" +
          request.getServerPort() + AJAX_REDIRECT_PATH;
      ObjectNode node = mapper.createObjectNode();
      node.put("knoxSSORedirectURL", constructLoginURL(ajaxReturnUrl));
      response.setContentType("application/json");
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED, node.toString());
    } else {
      StringBuffer urlBuilder = request.getRequestURL();
      if (request.getQueryString() != null) {
        urlBuilder.append('?').append(request.getQueryString());
      }
      response.sendRedirect(constructLoginURL(urlBuilder.toString()));
    }
  }

  private String constructLoginURL(String appReturnUrl) throws ServletException {
    try {
      URIBuilder loginUriBuilder = new URIBuilder(authConfig.getKnoxSSOUrl());
      loginUriBuilder.addParameter(authConfig.getKnoxUrlParamName(), appReturnUrl);
      return loginUriBuilder.build().toString();
    } catch (URISyntaxException e) {
      log.error("Unexpected exception building URI", e);
      throw new ServletException(e);
    }
  }

  private static RSAPublicKey toRSAKey(String pem) throws ServletException {
    String pemHeader = "-----BEGIN CERTIFICATE-----\n";
    String pemFooter = "\n-----END CERTIFICATE-----";
    String fullPem = pem.startsWith(pemHeader) ? pem : pemHeader + pem + pemFooter;
    try {
      CertificateFactory fact = CertificateFactory.getInstance("X.509");
      ByteArrayInputStream is = new ByteArrayInputStream(fullPem.getBytes("UTF8"));
      X509Certificate cer = (X509Certificate) fact.generateCertificate(is);
      return (RSAPublicKey)cer.getPublicKey();
    } catch (CertificateException e) {
      log.error("Got CertificateException: {}", e);
      throw new ServletException("Got CertificateException - PEM may be corrupt", e);
    } catch (UnsupportedEncodingException e) {
      log.error("Got UnsupportedEncodingException: {}", e);
      throw new ServletException(e);
    }
  }
}
