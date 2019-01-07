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
package com.hortonworks.hivestudio.common.command;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

import com.hortonworks.hivestudio.common.Constants;
import org.apache.commons.lang3.StringUtils;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.FileTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * Dropwizard command to setup configuration in production run
 */
public class SetupCommand extends Command {

  private final String DEFAULT_TEMPLATE_LOCATION;
  private final String DEFAULT_CONFIG_DIRECTORY;
  private final List<SetupConfiguration> setupConfigurations;

  public SetupCommand(List<SetupConfiguration> setupConfigurations) {
    this(setupConfigurations, "/etc/das/conf/template/production.json.hbs", Constants.DEFAULT_CONFIG_DIR);
  }

  public SetupCommand(List<SetupConfiguration> setupConfigurations, String defaultTemplateLocation, String defaultConfigDirectory) {
    super("setup", "Sets up production configuration for Hive Studio");
    DEFAULT_TEMPLATE_LOCATION = defaultTemplateLocation;
    DEFAULT_CONFIG_DIRECTORY = defaultConfigDirectory;
    this.setupConfigurations = setupConfigurations;
  }

  @Override
  public void configure(Subparser subparser) {
    subparser.addArgument("-t", "--template")
      .dest("template")
      .type(String.class)
      .required(false)
      .help("Template file location");

    subparser.addArgument("-l", "--location")
      .dest("location")
      .type(String.class)
      .required(false)
      .help("Directory where the configuration file will be generated");

  }

  @Override
  public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
    String templateFile = Optional.ofNullable(namespace.getString("template")).orElse(DEFAULT_TEMPLATE_LOCATION);
    String configLocation = Optional.ofNullable(namespace.getString("location")).orElse(DEFAULT_CONFIG_DIRECTORY);

    String templateFileBaseDir = templateFile.substring(0, templateFile.lastIndexOf('/'));
    String templateFileSuffix = templateFile.substring(templateFile.lastIndexOf('.') + 1);
    String templateFileName = templateFile.substring(templateFile.lastIndexOf('/'), templateFile.lastIndexOf('.'));

    Map<String, String> context = getUserInput();
    try (Writer writer = new BufferedWriter(new FileWriter(configLocation + File.separator + templateFileName))) {
      TemplateLoader loader = new FileTemplateLoader(templateFileBaseDir, templateFileSuffix);
      Handlebars handlebars = new Handlebars(loader);
      Template template = handlebars.compile(templateFileName + ".");
      template.apply(context, writer);
    }
  }

  private Map<String, String> getUserInput() {
    Map<String, String> context = new HashMap<>();
    final Scanner scanner = new Scanner(System.in);
    setupConfigurations.forEach( def -> {

      while (context.get(def.getConfigName()) == null) {
        System.out.print(def.getDisplayText());
        if(def.getDefaultValue().isPresent()) {
          System.out.print(" (" + def.getDefaultValue().get() + ") ");
        }
        System.out.print(": ");

        String enteredValue = scanner.nextLine();
        if (!StringUtils.isEmpty(enteredValue)) {
          context.put(def.getConfigName(), enteredValue.trim());
        } else {
          if (def.isRequired() && !def.getDefaultValue().isPresent()) {
            System.out.println(def.getConfigName() + " is required and has no default value. Please enter");
          } else {
            context.put(def.getConfigName(), def.getDefaultValue().get());
          }
        }
      }
    });
    scanner.close();
    return context;
  }
}
