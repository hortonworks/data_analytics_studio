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
package com.hortonworks.hivestudio.webapp.modules;

import java.util.Optional;
import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.ws.rs.client.Client;

import com.hortonworks.hivestudio.common.config.DASDropwizardConfiguration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import org.jdbi.v3.core.Jdbi;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.AuthConfig;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.config.HiveStudioDefaults;
import com.hortonworks.hivestudio.common.util.PropertyUtils;
import com.hortonworks.hivestudio.webapp.AppConfiguration;

import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebappModule extends AbstractModule{

  private final AppConfiguration configuration;
  private final Environment environment;
  private String configDir;
  private Jdbi jdbi;

  public WebappModule(AppConfiguration configuration, Environment environment, String configDir, Jdbi jdbi){
    this.configuration = configuration;
    this.environment = environment;
    this.configDir = configDir;
    this.jdbi = jdbi;
  }

  @Override
  protected void configure() {
    Optional<InitialDirContext> initialDirContextOptional = initLdap(configuration.getAuthConfig());
    initialDirContextOptional.ifPresent(initialDirContext -> bind(InitialDirContext.class).toInstance(initialDirContext));
  }

  private Optional<InitialDirContext> initLdap(AuthConfig authConfig) {
    if (authConfig.isLdapSSOEnabled()) {
      log.info("Initialized LDAP SSO.");
      Properties props = new Properties();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      props.put(Context.PROVIDER_URL, authConfig.getLdapUrl()); // "ldap://localhost:33389"
      props.put(Context.SECURITY_PRINCIPAL, authConfig.getLdapSecurityPrinciple()); // "uid=admin,ou=people,dc=hadoop,dc=apache,dc=org" //adminuser - User with special priviledge, dn user
      props.put(Context.SECURITY_CREDENTIALS,authConfig.getLdapPassword() );//"admin-password" //dn user password
      try {
        return Optional.of(new InitialDirContext(props));
      } catch (NamingException e) {
        log.error("exception occurred while contacting the ldap server : {}", props.getProperty(Context.PROVIDER_URL), e);
        throw new ServiceFormattedException("exception occurred while contacting the ldap server : " + props.getProperty(Context.PROVIDER_URL));
      }
    }

    return Optional.empty();
  }
  @Provides
  public Jdbi provideJdbi(){
    return jdbi;
  }

  @Provides
  @Singleton
  public AppConfiguration provideAppConfiguration(){
    return configuration;
  }

  @Provides
  @Singleton
  public DASDropwizardConfiguration provideDASDropwizardConfiguration(){
    return configuration;
  }

  @Provides
  public AuthConfig provideAuthConfig() {
    return configuration.getAuthConfig();
  }

  @Provides
  @Singleton
  @Inject
  public Configuration provideConfiguration(PropertyUtils propertyUtils){
    // load default properties
    Properties properties = new Properties(HiveStudioDefaults.getDefaultConfigurations());
    propertyUtils.readPropertyFile(properties, this.configDir, Constants.CONFIG_FILE_NAME);

    log.debug("all configurations for hive studio : {}", properties);
    return new Configuration(properties);
  }

  @Singleton
  @Provides
  public Client provideJerseyClient(){
    final Client client = new JerseyClientBuilder(environment).using(configuration.getJerseyClientConfiguration())
        .build(WebappModule.class.getName());

    return client;
  }
}
