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

package com.hortonworks.hivestudio.webapp.resources;

import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.AuthConfig;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import javax.inject.Inject;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.Properties;

@Slf4j
@Path("/login")
public class LdapLoginResource {

  private InitialDirContext initialDirContext;
  private AuthConfig authConfig;

  @Inject
  public LdapLoginResource(InitialDirContext initialDirContext, AuthConfig authConfig){
    this.initialDirContext = initialDirContext;
    this.authConfig = authConfig;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public void login(LoginRequest loginRequest, @Context HttpServletRequest httpServletRequest) {
    HttpSession session = httpServletRequest.getSession();
    String username = (String) session.getAttribute(Constants.SESSION_USER_KEY);
    if(Strings.isNullOrEmpty(username)){
      if( verifyFromLdap(loginRequest) ){
        session.setAttribute(Constants.SESSION_USER_KEY, loginRequest.getUsername());
      }else{
        throw new ServiceFormattedException("Incorrect username or password.");
      }
    }else{
      throw new ServiceFormattedException("User '" + username + " already logged in.");
    }
  }

  private boolean verifyFromLdap(LoginRequest loginRequest) {
    try {
      return authenticateJndi(loginRequest.getUsername(), loginRequest.getPassword());
    } catch (NamingException e) {
      throw new ServiceFormattedException(e);
    }
  }

  public boolean  authenticateJndi(String username, String password) throws NamingException {

    SearchControls ctrls = new SearchControls();
    ctrls.setReturningAttributes(new String[] { "givenName", "sn","memberOf" });
    ctrls.setSearchScope(SearchControls.SUBTREE_SCOPE);

    NamingEnumeration<SearchResult> answers = initialDirContext.search("ou=people,dc=hadoop,dc=apache,dc=org", "(uid=" + username + ")", ctrls);
    if(null == answers){
      return false;
    }
    javax.naming.directory.SearchResult result = answers.nextElement();

    String user = result.getNameInNamespace();

    try {
      Properties props = new Properties();
      props.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      props.put(javax.naming.Context.PROVIDER_URL, authConfig.getLdapUrl());
      props.put(javax.naming.Context.SECURITY_PRINCIPAL, user);
      props.put(javax.naming.Context.SECURITY_CREDENTIALS, password);

      InitialDirContext userContext = new InitialDirContext(props);
    } catch (Exception e) {
      log.error("Exception occurred while login in LDAP ", e);
      return false;
    }
    return true;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class LoginRequest{
    private String username;
    private String password;
  }

}
