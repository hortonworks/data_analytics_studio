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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.config.HiveInteractiveConfiguration;
import com.hortonworks.hivestudio.common.dto.ServiceConfig;
import com.hortonworks.hivestudio.common.resource.AdminOnly;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import com.hortonworks.hivestudio.webapp.AppConfiguration;
import com.hortonworks.hivestudio.webapp.configuration.DASConfig;
import com.hortonworks.hivestudio.webapp.dto.ProductInformation;
import com.hortonworks.hivestudio.webapp.services.AboutService;

/**
 * User related info service
 */
@Path("/about")
public class AboutResource {
  private AboutService aboutService;
  private AppConfiguration appConfig;
  private final Configuration configuration;
  private final HiveConfiguration hiveConfiguration;
  private final HiveInteractiveConfiguration hiveInteractiveConfiguration;
  private ObjectMapper objectMapper;


  @Inject
  public AboutResource(AboutService aboutService, AppConfiguration config, Configuration configuration,
                       HiveConfiguration hiveConfiguration, HiveInteractiveConfiguration hiveInteractiveConfiguration, ObjectMapper objectMapper) {
    this.aboutService = aboutService;
    this.appConfig = config;
    this.configuration = configuration;
    this.hiveConfiguration = hiveConfiguration;
    this.hiveInteractiveConfiguration = hiveInteractiveConfiguration;
    this.objectMapper = objectMapper;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response about(@Context RequestContext requestContext) throws IOException {
    ProductInformation productDetails = aboutService.getProductDetails();
    Map<String, Object> map = new HashMap<>();
    map.put("info", productDetails);
    return Response.ok(map).build();
  }

  @Path("/context")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response info(@Context RequestContext requestContext) {
    Map<String, Object> map = new HashMap<>();
    String smartsenseId = StringUtils.isNullOrEmpty(appConfig.getSmartsenseId()) ? "" : "yes";
    map.put("smartsense_id", smartsenseId);
    map.put("username", requestContext.getUsername());
    return Response.ok(map).build();
  }

  @AdminOnly
  @GET
  @Path("configs")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfigs(@Context RequestContext requestContext) throws JsonProcessingException {
      JsonNode jsonNode = objectMapper.valueToTree(this.appConfig);
      AppConfiguration configs = objectMapper.treeToValue(jsonNode, AppConfiguration.class);
      configs.getDatabase().setPassword("");
      ServiceConfig serviceConfig = new ServiceConfig(configuration, hiveConfiguration, hiveInteractiveConfiguration);
      DASConfig dasConfig = new DASConfig(configs, serviceConfig);
      return Response.ok(dasConfig).build();
  }
}
