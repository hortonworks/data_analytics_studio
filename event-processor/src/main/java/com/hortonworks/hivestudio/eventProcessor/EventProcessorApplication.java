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
package com.hortonworks.hivestudio.eventProcessor;

import java.util.EnumSet;
import java.util.List;

import javax.servlet.DispatcherType;

import org.jdbi.v3.core.Jdbi;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.command.SetupCommand;
import com.hortonworks.hivestudio.common.resource.HealthCheckResource;
import com.hortonworks.hivestudio.common.util.DatabaseHealthCheck;
import com.hortonworks.hivestudio.common.util.FilterDefinition;
import com.hortonworks.hivestudio.eventProcessor.configuration.SetupConfigurationDefinitions;
import com.hortonworks.hivestudio.eventProcessor.lifecycle.ActorSystemManager;
import com.hortonworks.hivestudio.eventProcessor.lifecycle.EventProcessorManager;
import com.hortonworks.hivestudio.eventProcessor.lifecycle.ReportingSchedulerManager;
import com.hortonworks.hivestudio.eventProcessor.registries.FilterRegistry;
import com.hortonworks.hivestudio.eventProcessor.registries.GuiceModuleRegistry;
import com.hortonworks.hivestudio.eventProcessor.registries.RestResourcesClassRegistry;

import io.dropwizard.Application;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class EventProcessorApplication extends Application<EventProcessorConfiguration> {
  private final String DEFAULT_TEMPLATE_NAME = "das-event-processor.json.hbs";
  private final String DEFAULT_TEMPLATE_LOCATION = Constants.DEFAULT_TEMPLATE_DIR + "/" + DEFAULT_TEMPLATE_NAME;

  public static void main(String[] args) throws Exception {
    new EventProcessorApplication().run(args);
  }

  @Override
  public String getName() {
    return "Hive Studio Event Processor";
  }

  @Override
  public void initialize(Bootstrap<EventProcessorConfiguration> bootstrap) {
    bootstrap.addCommand(new SetupCommand(SetupConfigurationDefinitions.get(), DEFAULT_TEMPLATE_LOCATION, Constants.DEFAULT_CONFIG_DIR));
    bootstrap.getObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Override
  public void run(EventProcessorConfiguration configuration, Environment environment) throws Exception {
    AppAuthentication appAuthentication = new AppAuthentication(configuration.getAuthConfig());
    JdbiFactory factory = new JdbiFactory();
    Jdbi jdbi = factory.build(environment, configuration.getDatabase(), "postgresql");

    Injector injector = initializeGuice(configuration, environment, jdbi, appAuthentication);
    configureResources(injector, configuration, environment);
    configureServletFilters(injector, configuration, environment);

    environment.jersey().setUrlPattern("/api/*");

    environment.lifecycle().manage(new ActorSystemManager(injector));

    EventProcessorManager eventProcessorManager = injector.getInstance(EventProcessorManager.class);
    ReportingSchedulerManager reportingSchedulerManager = injector.getInstance(ReportingSchedulerManager.class);
    environment.lifecycle().manage(eventProcessorManager);
    environment.lifecycle().manage(reportingSchedulerManager);

    environment.healthChecks().register("database", injector.getInstance(DatabaseHealthCheck.class));
  }

  /**
   * Creates and returns the injector and registers the modules
   */
  private Injector initializeGuice(EventProcessorConfiguration configuration, Environment environment, Jdbi jdbi, AppAuthentication appAuth) {
    List<Module> modules = new GuiceModuleRegistry(configuration, environment, jdbi, appAuth).get();
    return Guice.createInjector(modules);
  }

  private void configureResources(Injector injector, EventProcessorConfiguration configuration, Environment environment) {
    JerseyEnvironment jersey = environment.jersey();
    new RestResourcesClassRegistry(configuration, environment).get().forEach(x -> jersey.register(injector.getInstance(x)));

    jersey.register(new HealthCheckResource(environment.healthChecks()));
  }

  private void configureServletFilters(Injector injector, EventProcessorConfiguration configuration, Environment environment) {
    List<FilterDefinition> filterDefinitions = new FilterRegistry(configuration, environment).get();
    ServletEnvironment servlets = environment.servlets();
    filterDefinitions.forEach(x -> servlets.addFilter(x.getName(), injector.getInstance(x.getFilterClass()))
      .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, x.getRoute())
    );
  }
}
