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
import com.hortonworks.hivestudio.common.entities.AppProperty;
import com.hortonworks.hivestudio.common.repository.AppPropertyRepository;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.client.DatabaseMetadataWrapper;
import com.hortonworks.hivestudio.hive.services.ConnectionFactory;
import com.hortonworks.hivestudio.hive.services.DDLProxy;
import com.hortonworks.hivestudio.webapp.AppConfiguration;
import com.hortonworks.hivestudio.webapp.dto.ProductInformation;
import com.hortonworks.hivestudio.webapp.resources.ResourceUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import javax.inject.Provider;
import java.io.IOException;
import java.util.Optional;

import static com.hortonworks.hivestudio.common.Constants.CLUSTER_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class AboutServiceTest {

  AboutService aboutService;

  DDLProxy proxy;
  ResourceUtils resourceUtils;
  AppConfiguration configuration;
  ConnectionFactory connectionFactory;
  Provider<AppPropertyRepository> appPropRepoProvider;
  AppPropertyRepository appPropRepo;
  private AppAuthentication appAuthentication;

  @Captor
  private ArgumentCaptor<AppProperty> captor;

  // Another util function to avoid warnings, should move to some common test framework.
  @SuppressWarnings("unchecked")
  private static <T> Provider<T> getMockProvider(Class<T> clazz) {
    return mock(Provider.class);
  }

  @Before
  public void setup() {
    proxy = mock(DDLProxy.class);
    resourceUtils = mock(ResourceUtils.class);
    configuration = mock(AppConfiguration.class);
    connectionFactory = mock(ConnectionFactory.class);
    appPropRepoProvider = getMockProvider(AppPropertyRepository.class);
    appPropRepo = mock(AppPropertyRepository.class);
    appAuthentication = mock(AppAuthentication.class);
    when(appPropRepoProvider.get()).thenReturn(appPropRepo);
    aboutService = new AboutService(proxy, resourceUtils, configuration, connectionFactory, appPropRepoProvider, appAuthentication);
  }

  @Test
  public void getOrCreateClusterId() {
    String clusterId = "Some-Cluster-Id";
    AppProperty clusterIdProp = new AppProperty(CLUSTER_ID, clusterId);
    when(appPropRepo.getByPropertyName(CLUSTER_ID)).thenReturn(Optional.of(clusterIdProp));

    AppProperty receivedClusterId = aboutService.getOrCreateClusterId();

    Assert.assertEquals("ClusterId returned was incorrect.", receivedClusterId.getPropertyValue(), clusterId);
    Assert.assertEquals("Returned property was not same as requested", receivedClusterId.getPropertyName(), CLUSTER_ID);
  }

  @Test
  public void getOrCreateClusterIdWithConstraintVoilationException() {
    String clusterId = "Some-Cluster-Id";
    AppProperty clusterIdProp = new AppProperty(CLUSTER_ID, clusterId);

    when(appPropRepo.getByPropertyName(CLUSTER_ID)).thenReturn(Optional.empty()).thenReturn(Optional.of(clusterIdProp));
    when(appPropRepo.save(any())).thenThrow(
       new RuntimeException(null, new PSQLException("ERROR: duplicate key value violates unique constraint \"app_properties_property_name_key\"", PSQLState.UNKNOWN_STATE))
    );

    AppProperty receivedClusterId = aboutService.getOrCreateClusterId();
    log.info("receivedClusterId : {}", receivedClusterId);

    Assert.assertNotNull("ClusterId retruned was null.", receivedClusterId.getPropertyValue());
    Assert.assertEquals("Returned property was not same as requested", receivedClusterId.getPropertyName(), CLUSTER_ID);
  }

  @Test(expected = Exception.class)
  public void getOrCreateClusterIdWithOtherException() {
    when(appPropRepo.getByPropertyName(CLUSTER_ID)).thenReturn(Optional.empty());
    when(appPropRepo.save(any())).thenThrow(new Exception());

    aboutService.getOrCreateClusterId();
  }

  @Test
  public void testProductVersionDev() throws IOException {
    when(configuration.getEnvironment()).thenReturn("development");
    when(appPropRepo.getByPropertyName(CLUSTER_ID))
        .thenReturn(Optional.of(new AppProperty(CLUSTER_ID, "test")));
    when(proxy.getDatabaseMetaInformation(any()))
        .thenReturn(new DatabaseMetadataWrapper(1, 0, "test", "v1"));
    RequestContext requestContext = new RequestContext(1l, "admin", AppAuthentication.Role.USER, null);
    HiveContext hiveContext = new HiveContext(requestContext.getUsername(), requestContext.getConnenctionUrl());
    when(resourceUtils.createHiveContext(requestContext)).thenReturn(hiveContext);
    ProductInformation details = aboutService.getProductDetails();
    Assert.assertEquals("Mismatch", "development", details.getProductVersion());
    verify(appAuthentication,times(1)).getAppUser();
  }

  @Test
  public void testProductVersionProd() throws IOException {
    when(configuration.getEnvironment()).thenReturn("production");
    when(appPropRepo.getByPropertyName(CLUSTER_ID))
        .thenReturn(Optional.of(new AppProperty(CLUSTER_ID, "test")));
    when(proxy.getDatabaseMetaInformation(any()))
        .thenReturn(new DatabaseMetadataWrapper(1, 0, "test", "v1"));
    ProductInformation details = aboutService.getProductDetails();
    // The version changes in the builds, just checking for not null.
    Assert.assertNotNull(details.getProductVersion());
  }

  @Test
  public void testConnectionUrl() throws IOException {
    String myConnUrl = "my-connection-url";
    String admin = "admin";
    RequestContext requestContext = new RequestContext(1l, admin, AppAuthentication.Role.USER, myConnUrl);
    HiveContext hiveContext = new HiveContext(admin, myConnUrl);
    when(resourceUtils.createHiveContext(requestContext)).thenReturn(hiveContext);
    when(connectionFactory.createJdbcUrl(hiveContext)).thenReturn(myConnUrl);
    String connectionUrl = aboutService.getConnectionDetails(requestContext);
    Assert.assertEquals(myConnUrl, connectionUrl);
    verify(connectionFactory,times(1)).createJdbcUrl(hiveContext);
  }
}
