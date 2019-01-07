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
package com.hortonworks.hivestudio.hive.services;

import static com.hortonworks.hivestudio.common.Constants.HIVE_DYNAMIC_SERVICE_DISCOVERY_KEY;
import static com.hortonworks.hivestudio.common.Constants.HIVE_ZOOKEEPER_QUORUM_KEY;
import static com.hortonworks.hivestudio.common.Constants.HIVE_ZOOKEEPER_QUORUM_NAMESPACE_KEY;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.config.DASDropwizardConfiguration;
import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.config.HiveInteractiveConfiguration;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.client.ConnectionConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class ConnectionFactory {

  private HiveConfiguration hiveConfiguration;
  private Configuration appConfiguration;
  private HiveInteractiveConfiguration hiveInteractiveConfiguration;
  private ConnectionSystem connectionSystem;
  private DASDropwizardConfiguration dropwizardConfiguration;

  @Inject
  public ConnectionFactory(HiveConfiguration hiveConfiguration, Configuration appConfiguration,
                           HiveInteractiveConfiguration hiveInteractiveConfiguration, ConnectionSystem connectionSystem,
                           DASDropwizardConfiguration dropwizardConfiguration) {
    this.hiveConfiguration = hiveConfiguration;
    this.appConfiguration = appConfiguration;
    this.hiveInteractiveConfiguration = hiveInteractiveConfiguration;
    this.connectionSystem = connectionSystem;
    this.dropwizardConfiguration = dropwizardConfiguration;
  }

  public boolean isLdapEnabled(){
    Optional<String> authMode = hiveConfiguration.get(Constants.HIVE_AUTH_MODE);
    if(authMode.isPresent()){
      return authMode.get().equalsIgnoreCase("ldap");
    }else{
      return false;
    }
  }

  public ConnectionConfig create(HiveContext context)  {
    String jdbcUrl = createJdbcUrl(context);
    jdbcUrl =  appendSessionParams(context, jdbcUrl);
    if(isLdapEnabled()){
      Optional<String> opPassword = connectionSystem.getPassword(context);
      if(opPassword.isPresent()){
        return new ConnectionConfig(context, opPassword.get(), jdbcUrl);
      }
    }
    log.info("jdbcUrl for hive = {}", jdbcUrl);
    return new ConnectionConfig(context, "", jdbcUrl);
  }

  public String createJdbcUrl(HiveContext context) {
    String jdbcUrl;
    if(Strings.isNullOrEmpty(context.getConnectionUrl())) {
      if (zookeeperConfigured()) {
        jdbcUrl = getFromClusterZookeeperConfig(context);
      } else {
        jdbcUrl = getFromHiveConfiguration(context);
      }
    }else{
      jdbcUrl = context.getConnectionUrl();
    }
    return jdbcUrl;
  }


  private boolean isLLAPEnabled(Configuration configuration){
    return configuration.get(Constants.USE_HIVE_INTERACTIVE_MODE, "false").equalsIgnoreCase("true");
  }

  private String getFromHiveConfiguration(HiveContext context) {
    boolean useLLAP = isLLAPEnabled(appConfiguration);
    String transportMode = hiveConfiguration.getOrThrow(Constants.HIVE_TRANSPORT_MODE_KEY);
    String binaryPort = hiveConfiguration.getOrThrow(Constants.BINARY_PORT_KEY);
    String httpPort = hiveConfiguration.getOrThrow(Constants.HTTP_PORT_KEY);
    if (useLLAP) {
      binaryPort = hiveInteractiveConfiguration.getOrThrow(Constants.BINARY_PORT_KEY);
      httpPort = hiveInteractiveConfiguration.getOrThrow(Constants.HTTP_PORT_KEY);
    }

    String pathKey = hiveConfiguration.getOrThrow(Constants.HTTP_PATH_KEY);
    List<String> hiveHosts = Arrays.asList(appConfiguration.getOrThrow(Constants.AMBARI_HIVE_SERVICE_NAME + "." + Constants.AMBARI_HIVESERVER_COMPONENT_NAME).split(","));

    boolean isBinary = transportMode.equalsIgnoreCase("binary");
    final String port = isBinary ? binaryPort : httpPort;

    List<String> hostPorts = hiveHosts.stream().map(input -> input + ":" + port).collect(Collectors.toList());

    String concatHostPorts = Joiner.on(",").join(hostPorts);

    StringBuilder builder = new StringBuilder();
    builder.append("jdbc:hive2://")
        .append(concatHostPorts).append("/");

    if (!isBinary) {
      builder.append(";").append("transportMode=http;httpPath=").append(pathKey);
    }

    return builder.toString();
  }

  private String getFromClusterZookeeperConfig(HiveContext context) {
    boolean useLLAP = isLLAPEnabled(appConfiguration);
    String quorum = hiveConfiguration.getOrThrow(HIVE_ZOOKEEPER_QUORUM_KEY);

    String namespace = hiveConfiguration.getOrThrow(HIVE_ZOOKEEPER_QUORUM_NAMESPACE_KEY);
    if (useLLAP) {
      namespace = hiveInteractiveConfiguration.getOrThrow(HIVE_ZOOKEEPER_QUORUM_NAMESPACE_KEY);
    }

    String jdbcUrl = String.format("jdbc:hive2://%s/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=%s", quorum, namespace);

    return jdbcUrl;
  }

  public String appendSessionParams(HiveContext context, String jdbcUrl) {
    String sessionParams = dropwizardConfiguration.getHiveSessionParams();

    if (Strings.isNullOrEmpty(sessionParams)) {
      sessionParams = "";
    }

    if (!sessionParams.contains(Constants.HS2_PROXY_USER)) {
      if (!sessionParams.isEmpty()) {
        sessionParams += ";";
      }
      sessionParams = sessionParams + Constants.HS2_PROXY_USER + "=" + context.getUsername();
    }

    if (sessionParams.isEmpty()) {
      return jdbcUrl;
    }
    return jdbcUrl + ";" + sessionParams;
  }

  private boolean zookeeperConfigured() {
    boolean fromHiveSite = Boolean.valueOf(hiveConfiguration.getOrThrow(HIVE_DYNAMIC_SERVICE_DISCOVERY_KEY));
    boolean fromHiveInteractiveSite = Boolean.valueOf(hiveInteractiveConfiguration.getOrThrow(HIVE_DYNAMIC_SERVICE_DISCOVERY_KEY));
    return fromHiveInteractiveSite || fromHiveSite;
  }

  private String getConnectFromCustom() {
    String jdbcUrl = appConfiguration.getOrThrow(Constants.HIVE_JDBC_URL_KEY);
    String hiveSessionParams = dropwizardConfiguration.getHiveSessionParams();
    return jdbcUrl + ";" + hiveSessionParams;
  }
}
