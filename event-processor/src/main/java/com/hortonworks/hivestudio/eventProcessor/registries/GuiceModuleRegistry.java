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
package com.hortonworks.hivestudio.eventProcessor.registries;

import org.jdbi.v3.core.Jdbi;

import com.google.inject.Module;
import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.module.CommonModule;
import com.hortonworks.hivestudio.common.module.ConfigurationModule;
import com.hortonworks.hivestudio.common.util.ApplicationRegistry;
import com.hortonworks.hivestudio.eventProcessor.EventProcessorConfiguration;
import com.hortonworks.hivestudio.eventProcessor.module.EventProcessorModule;
import com.hortonworks.hivestudio.query.modules.SearchParserModule;
import com.hortonworks.hivestudio.reporting.GuiceReportModule;

import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GuiceModuleRegistry extends ApplicationRegistry<Module, EventProcessorConfiguration> {
  private final Jdbi jdbi;
  private final AppAuthentication appAuth;

  public GuiceModuleRegistry(EventProcessorConfiguration configuration, Environment environment,
      Jdbi jdbi, AppAuthentication appAuth) {
    super(configuration, environment);
    this.jdbi = jdbi;
    this.appAuth = appAuth;
  }

  @Override
  protected void register(EventProcessorConfiguration configuration, Environment environment) {
    add(new CommonModule(jdbi, appAuth));
    add(new SearchParserModule());
    add(new GuiceReportModule());
    String configDir = configuration.getServiceConfigDirectory();
    log.info("provided configuration directory : {}", configDir);
    ConfigurationModule configurationModule = new ConfigurationModule();
    add(configurationModule);
    add(new EventProcessorModule(configuration, environment, configDir, jdbi));
  }
}
