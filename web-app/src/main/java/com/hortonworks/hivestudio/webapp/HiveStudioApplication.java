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
package com.hortonworks.hivestudio.webapp;

import java.util.EnumSet;
import java.util.List;
import javax.servlet.DispatcherType;
import javax.ws.rs.Priorities;

import com.hortonworks.hivestudio.common.util.PasswordSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StrSubstitutor;
import org.eclipse.jetty.server.session.SessionHandler;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.jdbi.v3.core.Jdbi;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.command.SetupCommand;
import com.hortonworks.hivestudio.common.config.AuthConfig;
import com.hortonworks.hivestudio.common.resource.HealthCheckResource;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import com.hortonworks.hivestudio.common.util.DatabaseHealthCheck;
import com.hortonworks.hivestudio.common.util.FilterDefinition;
import com.hortonworks.hivestudio.webapp.bundle.DefaultFlywayBundle;
import com.hortonworks.hivestudio.webapp.configuration.SetupConfigurationDefinitions;
import com.hortonworks.hivestudio.webapp.filters.CSRFFilter;
import com.hortonworks.hivestudio.webapp.filters.RequestContextFilter;
import com.hortonworks.hivestudio.webapp.mapper.exception.ServiceFormattedExceptionMapper;
import com.hortonworks.hivestudio.webapp.modules.RequestContextFactory;
import com.hortonworks.hivestudio.webapp.registries.FilterRegistry;
import com.hortonworks.hivestudio.webapp.registries.GuiceModuleRegistry;
import com.hortonworks.hivestudio.webapp.registries.RestResourcesClassRegistry;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

@Slf4j
public class HiveStudioApplication extends Application<AppConfiguration> {

  private static final String HIVESTUDIO_TEMPLATE_DIR = Constants.DEFAULT_CONFIG_DIR + "/template/web-app";
  private static final String HIVESTUDIO_TEMPLATE_NAME = "das-webapp.json.hbs";
  private static final String HIVESTUDIO_TEMPLATE_LOCATION = HIVESTUDIO_TEMPLATE_DIR + "/" + HIVESTUDIO_TEMPLATE_NAME;

  public static void main(final String[] args) throws Exception {
    new HiveStudioApplication().run(args);
  }

  @Override
  public String getName() {
    return "Hive Studio";
  }

  @Override
  public void initialize(final Bootstrap<AppConfiguration> bootstrap) {
    if (shouldEnableAssets()) {
      bootstrap.addBundle(new AssetsBundle("/assets", "/", "index.html", "assets"));
    }
    bootstrap.addBundle(new DefaultFlywayBundle());

    bootstrap.addCommand(new SetupCommand(SetupConfigurationDefinitions.get(), HIVESTUDIO_TEMPLATE_LOCATION, Constants.DEFAULT_CONFIG_DIR));

    bootstrap.getObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
        new PasswordSubstitutor()
      )
    );
  }

  @Override
  public void run(AppConfiguration configuration, Environment environment) throws Exception {
    AppAuthentication appAuth = new AppAuthentication(configuration.getAuthConfig());

    JdbiFactory factory = new JdbiFactory();
    Jdbi jdbi = factory.build(environment, configuration.getDatabase(), "postgresql");

    Injector injector = initializeGuice(configuration, environment, jdbi, appAuth);

    environment.servlets().setSessionHandler(new SessionHandler());
    // web resources
    configureResources(injector, configuration, environment, appAuth);
    configureServletFilters(injector, configuration, environment);
    environment.jersey().setUrlPattern("/api/*");

    environment.jersey().register(new ServiceFormattedExceptionMapper());
    environment.jersey().register(MultiPartFeature.class);

    environment.healthChecks().register("database", injector.getInstance(DatabaseHealthCheck.class));
  }

  protected boolean shouldEnableAssets() {
    return true;
  }

  /**
   * Creates and returns the injector and registers the modules
   */
  private Injector initializeGuice(AppConfiguration configuration, Environment environment, Jdbi jdbi, AppAuthentication appAuth) {
    List<Module> modules = new GuiceModuleRegistry(configuration, environment, jdbi, appAuth).get();
    return Guice.createInjector(modules);
  }

  private void configureResources(Injector injector, AppConfiguration configuration,
      Environment environment, AppAuthentication appAuth) {
    JerseyEnvironment jersey = environment.jersey();

    new RestResourcesClassRegistry(configuration, environment).get()
        .forEach(x -> jersey.register(injector.getInstance(x)));

    jersey.register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(configuration.getAuthConfig()).to(AuthConfig.class);
        bind(appAuth).to(AppAuthentication.class);
        bindFactory(RequestContextFactory.class).to(RequestContext.class).in(RequestScoped.class);
      }
    });

    jersey.register(new HealthCheckResource(environment.healthChecks()));
  }

  private void configureServletFilters(Injector injector, AppConfiguration configuration, Environment environment) {
    // There is no documented ordering b/w servlet filter and ContainerRequestFilter, works by experiment.

    // Servlet Filter which is used by jersey for the entire container: all calls including static resources.
    List<FilterDefinition> filterDefinitions = new FilterRegistry(configuration, environment).get();
    ServletEnvironment servlets = environment.servlets();
    filterDefinitions.forEach(x -> servlets.addFilter(x.getName(), injector.getInstance(x.getFilterClass()))
      .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, x.getRoute())
    );

    // Resource filters invoked in the order of priority. only /api/
    DropwizardResourceConfig config = environment.jersey().getResourceConfig();
    config.register(RequestContextFilter.class, Priorities.AUTHORIZATION + 1);
    config.register(CSRFFilter.class, Priorities.AUTHORIZATION + 2);
  }
}
