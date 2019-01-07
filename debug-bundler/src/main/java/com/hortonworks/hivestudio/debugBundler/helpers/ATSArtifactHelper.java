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

package com.hortonworks.hivestudio.debugBundler.helpers;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.HttpArtifact;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;
import java.util.List;

@Singleton
public class ATSArtifactHelper {
  // This is a private field in yarn timeline client :-(.
  private static final String TIMELINE_PATH_PREFIX = "/ws/v1/timeline/";
  private static final String DOMAIN_PATH_PREFIX = "/ws/v1/timeline/domain/";

  private final YarnConfiguration conf;
  private final String atsAddress;

  public static class ATSEvent {
    public long timestamp;
    public String eventtype;
    // ignored eventinfo
  }
  public static class ATSLog {
    public List<ATSEvent> events;
    public String entitytype;
    public String entity;
    public long starttime;
    // ignored domain, relatedentities, primaryfilters, otherinfo.
  }

  @Inject
  public ATSArtifactHelper(YarnConfiguration conf) {
    this.conf = conf;

    String yarnHTTPPolicy = conf.get(YarnConfiguration.YARN_HTTP_POLICY_KEY,
      YarnConfiguration.YARN_HTTP_POLICY_DEFAULT);

    if (HttpConfig.Policy.HTTPS_ONLY == HttpConfig.Policy.fromString(yarnHTTPPolicy)) {
      atsAddress = "https://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS);
    } else {
      atsAddress = "http://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS);
    }
  }

  public Artifact getEntityArtifact(String name, String entityType, String entityId) {
    try {
      URIBuilder builder = new URIBuilder(atsAddress);
      builder.setPath(TIMELINE_PATH_PREFIX + entityType + "/" + entityId);
      String url = builder.build().toString();
      return new HttpArtifact(conf, name, url, false);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid atsAddress: " + atsAddress, e);
    }
  }

  public Artifact getChildEntityArtifact(String name, String entityType, String rootEntityType,
                                         String rootEntityId) {
    return getChildEntityArtifact(name, entityType, rootEntityType, rootEntityId, false);
  }

  public Artifact getChildEntityArtifact(String name, String entityType, String rootEntityType,
    String rootEntityId, boolean isTemp) {
    try {
      URIBuilder builder = new URIBuilder(atsAddress);
      builder.setPath(TIMELINE_PATH_PREFIX + entityType);
      builder.setParameter("primaryFilter", rootEntityType + ":" + rootEntityId);
      String url = builder.build().toString();
      return new HttpArtifact(conf, name, url, isTemp);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid atsAddress: " + atsAddress, e);
    }
  }

  public Artifact getDomainArtifact(String domainId, String name) {
    try {
      URIBuilder builder = new URIBuilder(atsAddress);
      builder.setPath(DOMAIN_PATH_PREFIX + domainId);
      String url = builder.build().toString();
      return new HttpArtifact(conf, name, url, true);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid atsAddress: " + atsAddress, e);
    }
  }
}
