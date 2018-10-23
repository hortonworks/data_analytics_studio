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

import com.hortonworks.hivestudio.hive.resources.jobs.atsJobs.TezVertexId;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Parser of Resource Manager responses
 */
@Slf4j
public class RMParser {
  private RMRequestsDelegate delegate;

  public RMParser(RMRequestsDelegate delegate) {
    this.delegate = delegate;
  }

  /**
   * Progress of DAG
   * @param appId App Id
   * @param dagId DAG Id
   * @return progress of DAG
   */
  public Double getDAGProgress(String appId, String dagId) {
    String dagIdx = parseDagIdIndex(dagId);
    JSONObject progresses = delegate.dagProgress(appId, dagIdx);

    double dagProgressValue;
    if (progresses != null) {
      JSONObject dagProgress = (JSONObject) progresses.get("dagProgress");
      dagProgressValue = (Double) (dagProgress.get("progress"));
    } else {
      log.error("Error while retrieving progress of " + appId + ":" + dagId + ". 0 assumed.");
      dagProgressValue = 0;
    }
    return dagProgressValue;
  }

  /**
   * Progress of vertices
   * @param appId App Id
   * @param dagId DAG Id
   * @param vertices vertices list
   * @return list of vertices
   */
  public List<VertexProgress> getDAGVerticesProgress(String appId, String dagId, List<TezVertexId> vertices) {
    String dagIdx = parseDagIdIndex(dagId);

    Map<String, String> vertexIdToEntityMapping = new HashMap<String, String>();
    StringBuilder builder = new StringBuilder();
    if (vertices.size() > 0) {
      for (TezVertexId vertexId : vertices) {
        String[] parts = vertexId.entity.split("_");
        String vertexIdx = parts[parts.length - 1];
        builder.append(vertexIdx).append(",");

        vertexIdToEntityMapping.put(vertexId.entity, vertexId.vertexName);
      }
      builder.setLength(builder.length() - 1); // remove last comma
    }

    String commaSeparatedVertices = builder.toString();

    List<VertexProgress> parsedVertexProgresses = new LinkedList<VertexProgress>();
    JSONObject vertexProgressesResponse = delegate.verticesProgress(
        appId, dagIdx, commaSeparatedVertices);
    if (vertexProgressesResponse == null) {
      log.error("Error while retrieving progress of vertices " +
          appId + ":" + dagId + ":" + commaSeparatedVertices + ". 0 assumed for all vertices.");
      for (TezVertexId vertexId : vertices) {
        VertexProgress vertexProgressInfo = new VertexProgress();
        vertexProgressInfo.name = vertexId.vertexName;
        vertexProgressInfo.progress = 0.0;
        parsedVertexProgresses.add(vertexProgressInfo);
      }
      return parsedVertexProgresses;
    }
    JSONArray vertexProgresses = (JSONArray) vertexProgressesResponse.get("vertexProgresses");

    for (Object vertex : vertexProgresses) {
      JSONObject jsonObject = (JSONObject) vertex;

      VertexProgress vertexProgressInfo = new VertexProgress();
      vertexProgressInfo.id = (String) jsonObject.get("id");
      vertexProgressInfo.name = vertexIdToEntityMapping.get(vertexProgressInfo.id);
      vertexProgressInfo.progress = (Double) jsonObject.get("progress");

      parsedVertexProgresses.add(vertexProgressInfo);
    }
    return parsedVertexProgresses;
  }

  public String parseDagIdIndex(String dagId) {
    String[] dagIdParts = dagId.split("_");
    return dagIdParts[dagIdParts.length - 1];
  }

  public static class VertexProgress {
    public String id;
    public String name;
    public Double progress;
  }
}
