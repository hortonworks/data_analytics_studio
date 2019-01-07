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


package com.hortonworks.hivestudio.hive.resources.jobs.atsJobs;

import com.hortonworks.hivestudio.common.RESTUtils;
import com.hortonworks.hivestudio.hive.HiveContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
@Slf4j
public class ATSRequestsDelegateImpl implements ATSRequestsDelegate {
  public static final String EMPTY_ENTITIES_JSON = "{ \"entities\" : [  ] }";

  private HiveContext context;
  private String atsUrl;

  public ATSRequestsDelegateImpl(HiveContext context, String atsUrl) {
    this.context = context;
    this.atsUrl = addProtocolIfMissing(atsUrl);
  }

  private String addProtocolIfMissing(String atsUrl) {
    if (!atsUrl.matches("^[^:]+://.*$"))
      atsUrl = "http://" + atsUrl;
    return atsUrl;
  }

  @Override
  public String hiveQueryIdDirectUrl(String entity) {
    return atsUrl + "/ws/v1/timeline/HIVE_QUERY_ID/" + entity;
  }

  @Override
  public String hiveQueryIdOperationIdUrl(String operationId) {
    // ATS parses operationId started with digit as integer and not returns the response.
    // Quotation prevents this.
    return atsUrl + "/ws/v1/timeline/HIVE_QUERY_ID?primaryFilter=operationid:%22" + operationId + "%22";
  }

  @Override
  public String tezDagDirectUrl(String entity) {
    return atsUrl + "/ws/v1/timeline/TEZ_DAG_ID/" + entity;
  }

  @Override
  public String tezDagNameUrl(String name) {
    return atsUrl + "/ws/v1/timeline/TEZ_DAG_ID?primaryFilter=dagName:" + name;
  }

  @Override
  public String tezVerticesListForDAGUrl(String dagId) {
    return atsUrl + "/ws/v1/timeline/TEZ_VERTEX_ID?primaryFilter=TEZ_DAG_ID:" + dagId;
  }

  @Override
  public JSONObject hiveQueryIdsForUser(String username) {
    String hiveQueriesListUrl = atsUrl + "/ws/v1/timeline/HIVE_QUERY_ID?primaryFilter=requestuser:" + username;
    String response = readFromWithDefault(hiveQueriesListUrl, "{ \"entities\" : [  ] }");
    return (JSONObject) JSONValue.parse(response);
  }

  @Override
  public JSONObject hiveQueryIdByOperationId(String operationId) {
    String hiveQueriesListUrl = hiveQueryIdOperationIdUrl(operationId);
    String response = readFromWithDefault(hiveQueriesListUrl, EMPTY_ENTITIES_JSON);
    return (JSONObject) JSONValue.parse(response);
  }

  @Override
  public JSONObject tezDagByName(String name) {
    String tezDagUrl = tezDagNameUrl(name);
    String response = readFromWithDefault(tezDagUrl, EMPTY_ENTITIES_JSON);
    return (JSONObject) JSONValue.parse(response);
  }

  @Override
  public JSONObject tezDagByEntity(String entity) {
    String tezDagEntityUrl = tezDagEntityUrl(entity);
    String response = readFromWithDefault(tezDagEntityUrl, EMPTY_ENTITIES_JSON);
    return (JSONObject) JSONValue.parse(response);
  }

  /**
   * fetches the HIVE_QUERY_ID from ATS for given user between given time period
   * @param username: username for which to fetch hive query IDs
   * @param startTime: time in miliseconds, inclusive
   * @param endTime: time in miliseconds, exclusive
   * @return
   */
  @Override
  public JSONObject hiveQueryIdsForUserByTime(String username, long startTime, long endTime) {
    StringBuilder url = new StringBuilder();
    url.append(atsUrl).append("/ws/v1/timeline/HIVE_QUERY_ID?")
      .append("windowStart=").append(startTime)
      .append("&windowEnd=").append(endTime)
      .append("&primaryFilter=requestuser:").append(username);
    String hiveQueriesListUrl = url.toString();

    String response = readFromWithDefault(hiveQueriesListUrl, EMPTY_ENTITIES_JSON);
    return (JSONObject) JSONValue.parse(response);
  }

  @Override
  public JSONObject hiveQueryEntityByEntityId(String hiveEntityId) {
    StringBuilder url = new StringBuilder();
    url.append(atsUrl).append("/ws/v1/timeline/HIVE_QUERY_ID/").append(hiveEntityId);
    String hiveQueriesListUrl = url.toString();
    String response = readFromWithDefault(hiveQueriesListUrl, EMPTY_ENTITIES_JSON);
    return (JSONObject) JSONValue.parse(response);
  }

  private String tezDagEntityUrl(String entity) {
    return atsUrl + "/ws/v1/timeline/TEZ_DAG_ID?primaryFilter=callerId:" + entity;
  }

  public boolean checkATSStatus() throws IOException {
    String url = atsUrl + "/ws/v1/timeline/";
    InputStream responseInputStream = RESTUtils.readAsCurrent(url, "GET",
            (String)null, new HashMap<String, String>());
     IOUtils.toString(responseInputStream);
    return true;
  }

  @Override
  public JSONObject tezVerticesListForDAG(String dagId) {
    String response = readFromWithDefault(tezVerticesListForDAGUrl(dagId), "{ \"entities\" : [  ] }");
    return (JSONObject) JSONValue.parse(response);
  }



  protected String readFromWithDefault(String atsUrl, String defaultResponse) {
    String response;
    try {
      InputStream responseInputStream = RESTUtils.readAsCurrent(atsUrl, "GET",
          (String)null, new HashMap<String, String>());
      response = IOUtils.toString(responseInputStream);
    } catch (IOException e) {
      log.error("Error while reading from ATS", e);
      response = defaultResponse;
    }
    return response;
  }

  public String getAtsUrl() {
    return atsUrl;
  }

  public void setAtsUrl(String atsUrl) {
    this.atsUrl = atsUrl;
  }
}
