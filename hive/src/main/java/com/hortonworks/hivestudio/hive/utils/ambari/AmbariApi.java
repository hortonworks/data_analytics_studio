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


package com.hortonworks.hivestudio.hive.utils.ambari;

import java.util.Map;

/**
 * Provides API to Ambari. Supports both Local and Remote cluster association.
 * Also provides API to get cluster topology (determine what node contains specific service)
 * on both local and remote cluster.
 */
public interface AmbariApi {

  /**
   *  Set requestedBy header
   *
   * @param requestedBy
   */
  public void setRequestedBy(String requestedBy);

  /**
   * Shortcut for GET method
   * @param path REST API path
   * @return response
   */
  public String requestClusterAPI(String path);

  /**
   * Request to Ambari REST API for current cluster. Supports both local and remote cluster
   * @param path REST API path after cluster name e.g. /api/v1/clusters/mycluster/[method]
   * @param method HTTP method
   * @param data HTTP data
   * @param headers HTTP headers
   * @return response
   */
  public String requestClusterAPI(String path, String method, String data, Map<String, String> headers);

  /**
   * Request to Ambari REST API. Supports both local and remote cluster
   * @param path REST API path, e.g. /api/v1/clusters/mycluster/
   * @param method HTTP method
   * @param data HTTP data
   * @param headers HTTP headers
   * @return response
   */
  public String readFromAmbari(String path, String method, String data, Map<String, String> headers);

  String getRMUrl();

  String getTimelineServerUrl();



  /**
   * Provides access to service-specific utilities
   * @return object with service-specific methods
   */
//  public Services getServices() {
//    if (services == null) {
//      services = new Services(this, context);
//    }
//    return services;
//  }
//
//  private Map<String,String> addRequestedByHeader(Map<String,String> headers){
//    if(headers == null){
//      headers = new HashMap<String, String>();
//    }
//
//    headers.put("X-Requested-By",this.requestedBy);
//
//    return headers;
//  }
//
//  /**
//   * Check if view is associated with cluster
//   *
//   * @return isClusterAssociated
//   */
//  public boolean isClusterAssociated(){
//    return context.getCluster() != null;
//  }

}
