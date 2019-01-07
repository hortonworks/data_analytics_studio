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
package com.hortonworks.hivestudio.webapp.services;

import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.entities.AppProperty;
import com.hortonworks.hivestudio.common.repository.AppPropertyRepository;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.client.DatabaseMetadataWrapper;
import com.hortonworks.hivestudio.hive.services.ConnectionFactory;
import com.hortonworks.hivestudio.hive.services.DDLProxy;
import com.hortonworks.hivestudio.webapp.AppConfiguration;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import com.hortonworks.hivestudio.webapp.dto.GAConfiguration;
import com.hortonworks.hivestudio.webapp.dto.ProductInformation;
import com.hortonworks.hivestudio.webapp.resources.ResourceUtils;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.postgresql.util.PSQLException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

@Singleton
@Slf4j
public class AboutService {
  private volatile ProductInformation productInfo = null;
  private final DDLProxy proxy;
  private final ResourceUtils resourceUtils;
  private final Configuration dasConf;
  private final AppConfiguration configuration;
  private ConnectionFactory connectionFactory;
  private Provider<AppPropertyRepository> appPropRepo;
  private final AppAuthentication appAuthentication;

  @Inject
  public AboutService(DDLProxy proxy, ResourceUtils resourceUtils, AppConfiguration configuration, ConnectionFactory connectionFactory,
                      Configuration dasConf, Provider<AppPropertyRepository> appPropRepo, AppAuthentication appAuthentication) {
    this.proxy = proxy;
    this.resourceUtils = resourceUtils;
    this.configuration = configuration;
    this.dasConf = dasConf;
    this.connectionFactory = connectionFactory;
    this.appPropRepo = appPropRepo;
    this.appAuthentication = appAuthentication;
  }

  private ProductInformation fetchProductInformation() throws IOException {
    HiveContext hiveContext = new HiveContext(appAuthentication.getAppUser());
    DatabaseMetadataWrapper databaseMetaInformation = proxy.getDatabaseMetaInformation(hiveContext);
    String productVersion = getProductVersion();
    String productName = getProductName();
    String description = getProductDescription();
    GAConfiguration gaDetails = getGoogleAnalyticsDetails();
    String clusterId = getClusterId();

    return ProductInformation.builder()
        .databaseProductName(databaseMetaInformation.getDatabaseProductName())
        .databaseProductVersion(databaseMetaInformation.getDatabaseProductVersion())
        .productName(productName)
        .productVersion(productVersion)
        .description(description)
        .gaDetails(gaDetails)
        .clusterId(clusterId)
        .build();
  }

  private String getClusterId() {
    AppProperty clusterId = getOrCreateClusterId();
    return clusterId.getPropertyValue();
  }

  public AppProperty getOrCreateClusterId() {
    Optional<AppProperty> appProperty = appPropRepo.get().getByPropertyName(Constants.CLUSTER_ID);
    if (!appProperty.isPresent()) {
      return createAndGetClusterId();
    } else {
      return appProperty.get();
    }
  }

  private AppProperty createAndGetClusterId() {
    AppProperty appProperty = new AppProperty(Constants.CLUSTER_ID, UUID.randomUUID().toString());
    log.info("Trying to save cluster id : {}", appProperty);
    try {
      return appPropRepo.get().save(appProperty);
    } catch (Exception ex) {
      if (null != ex.getCause() && ex.getCause() instanceof PSQLException) {
        String msg = ex.getCause().getMessage();
        if (null != msg && msg.contains("violates") && msg.contains("app_properties_property_name_key"))
          log.info("Failed to save cluster id. Retrying once more", ex);
        return getOrCreateClusterId();
      }
      throw ex;
    }
  }

  private GAConfiguration getGoogleAnalyticsDetails() {
    return configuration.getGaConfiguration();
  }

  private String getProductDescription() {
    return "";
  }

  private String getProductName() {
    return Constants.PRODUCT_NAME;
  }

  private String getProductVersion() throws IOException {
    if (configuration.getEnvironment().equalsIgnoreCase("development")) {
      return "development";
    }
    return dasConf.get(Constants.APPLICATION_VERSION, Constants.DEFAULT_VERSION_VAL);
  }

  public ProductInformation getProductDetails() throws IOException {
    if (null == productInfo) {
      synchronized (this) {
        if(null == productInfo)
        productInfo = fetchProductInformation();
      }
    }

    return productInfo;
  }

  public String getConnectionDetails(RequestContext requestContext) {
    HiveContext hiveContext = resourceUtils.createHiveContext(requestContext);
//    give the connection url being used by this user.
    return connectionFactory.createJdbcUrl(hiveContext);
  }
}
