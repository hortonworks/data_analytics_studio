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


package com.hortonworks.hivestudio.hive;

import com.hortonworks.hivestudio.common.config.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.hortonworks.hivestudio.common.Constants.HIVE_SESSION_PARAMS_KEY;

/**
 * Holds session parameters pulled from the
 * view context
 */
public class AuthParams {
  private Map<String, String> sessionParams = new HashMap<>();

  public AuthParams(Configuration configuration) {
    Optional<String> sessionParam = configuration.get(HIVE_SESSION_PARAMS_KEY);
    sessionParam.ifPresent(s -> sessionParams = parseSessionParams(s));
  }

  /**
   * Returns a map created by parsing the parameters in view context
   * @param params session parameters as string
   * @return parsed session parameters
   */
  private Map<String, String> parseSessionParams(String params) {
    Map<String, String> sessions = new HashMap<>();
    if (StringUtils.isEmpty(params))
      return sessions;
    String[] splits = params.split(";");
    for (String split : splits) {
      String[] paramSplit = split.trim().split("=");
      if ("auth".equals(paramSplit[0]) || "proxyuser".equals(paramSplit[0])) {
        sessions.put(paramSplit[0], paramSplit[1]);
      }
    }
    return Collections.unmodifiableMap(sessions);
  }

  /**
   * Gets the proxy user
   * @return User and group information
   * @throws IOException
   */
  public UserGroupInformation getProxyUser() throws IOException {
    UserGroupInformation ugi;
    String proxyuser = null;

    UserGroupInformation.isSecurityEnabled();
    // TODO : get the proxyuser before setting it
//    if(context.getCluster() != null) {
//      proxyuser = context.getCluster().getConfigurationValue("cluster-env","ambari_principal_name");
//    }

    if(StringUtils.isEmpty(proxyuser)) {
      if (sessionParams.containsKey("proxyuser")) {
        ugi = UserGroupInformation.createRemoteUser(sessionParams.get("proxyuser"));
      } else {
        ugi = UserGroupInformation.getCurrentUser();
      }
    } else {
      ugi = UserGroupInformation.createRemoteUser(proxyuser);
    }
    ugi.setAuthenticationMethod(getAuthenticationMethod());
    return ugi;
  }

  /**
   * Get the Authentication method
   * @return
   */
  private UserGroupInformation.AuthenticationMethod getAuthenticationMethod() {
    UserGroupInformation.AuthenticationMethod authMethod;
    if (sessionParams.containsKey("auth") && !StringUtils.isEmpty(sessionParams.get("auth"))) {
      String authName = sessionParams.get("auth");
      authMethod = UserGroupInformation.AuthenticationMethod.valueOf(authName.toUpperCase());
    } else {
      authMethod = UserGroupInformation.AuthenticationMethod.SIMPLE;
    }
    return authMethod;
  }
}
