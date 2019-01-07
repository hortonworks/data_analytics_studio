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


package com.hortonworks.hivestudio.hive.resources.jobs.rm;

import com.hortonworks.hivestudio.common.RESTUtils;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.hive.HiveContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
@Slf4j
public class RMRequestsDelegateImpl implements RMRequestsDelegate {
  public static final String EMPTY_ENTITIES_JSON = "{ \"entities\" : [  ] }";

  private HiveContext context;
  private String rmUrl;

  public RMRequestsDelegateImpl(HiveContext context, String rmUrl) {
    this.context = context;
    this.rmUrl = rmUrl;
  }

  @Override
  public String dagProgressUrl(String appId, String dagIdx) {
    return rmUrl + String.format("/proxy/%s/ws/v1/tez/dagProgress?dagID=%s", appId, dagIdx);
  }

  @Override
  public String verticesProgressUrl(String appId, String dagIdx, String vertices) {
    return rmUrl + String.format("/proxy/%s/ws/v1/tez/vertexProgresses?dagID=%s&vertexID=%s", appId, dagIdx, vertices);
  }

  @Override
  public JSONObject dagProgress(String appId, String dagIdx) {
    String url = dagProgressUrl(appId, dagIdx);
    String response;
    try {
      InputStream responseInputStream = RESTUtils.readFrom(url, "GET",
          (String)null, new HashMap<String, String>());
      response = IOUtils.toString(responseInputStream);
    } catch (IOException e) {
      throw new ServiceFormattedException(
          String.format("R010 DAG %s in app %s not found or ResourceManager is unreachable", dagIdx, appId));
    }
    return (JSONObject) JSONValue.parse(response);
  }

  @Override
  public JSONObject verticesProgress(String appId, String dagIdx, String commaSeparatedVertices) {
    String url = verticesProgressUrl(appId, dagIdx, commaSeparatedVertices);
    String response;
    try {
      InputStream responseInputStream = RESTUtils.readFrom(url, "GET",
          (String)null, new HashMap<String, String>());
      response = IOUtils.toString(responseInputStream);
    } catch (IOException e) {
      throw new ServiceFormattedException(
          String.format("R020 DAG %s in app %s not found or ResourceManager is unreachable", dagIdx, appId));
    }
    return (JSONObject) JSONValue.parse(response);
  }

  protected String readFromWithDefault(String url, String defaultResponse) {
    String response;
    try {
      InputStream responseInputStream = RESTUtils.readFrom(url, "GET",
          (String)null, new HashMap<String, String>());
      response = IOUtils.toString(responseInputStream);
    } catch (IOException e) {
      log.error("Error while reading from RM", e);
      response = defaultResponse;
    }
    return response;
  }

}
