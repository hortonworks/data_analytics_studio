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
package com.hortonworks.hivestudio.debugBundler.source;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.entities.VertexInfo;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactDownloadException;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSource;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.query.services.DagInfoService;
import com.hortonworks.hivestudio.query.services.HiveQueryService;
import com.hortonworks.hivestudio.query.services.QueryDetailsService;
import com.hortonworks.hivestudio.query.services.VertexInfoService;

public class HiveStudioArtifacts implements ArtifactSource {
  private static final String FILE_EXT = "json";

  private final HiveQueryService hiveQueryService;
  private final QueryDetailsService queryDetailsService;
  private final DagInfoService dagInfoService;
  private final VertexInfoService vertexInfoService;
  private final ObjectMapper objectMapper;

  @Inject
  public HiveStudioArtifacts(HiveQueryService hiveQueryService, DagInfoService dagInfoService,
      VertexInfoService vertexInfoService, QueryDetailsService queryDetailsService,
      ObjectMapper objectMapper) {
    this.hiveQueryService = hiveQueryService;
    this.dagInfoService = dagInfoService;
    this.queryDetailsService = queryDetailsService;
    this.vertexInfoService = vertexInfoService;
    this.objectMapper = objectMapper;
  }

  private void writeData(Path path, Map<String, Object> jsonObject) throws IOException {
    PrintWriter writer = new PrintWriter(Files.newBufferedWriter(path));
    writer.print(objectMapper.writeValueAsString(jsonObject));
    writer.flush();
    writer.close();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    String queryId = params.getHiveQueryId();
    HiveQuery query = hiveQueryService.getOneByHiveQueryId(queryId);

    Artifact queryArtifact = new Artifact() {
      @Override
      public boolean isTemp() {
        return false;
      }

      @Override
      public String getName() {
        return "DAS/QUERY." + FILE_EXT;
      }

      @Override
      public void downloadInto(Path path) throws ArtifactDownloadException {
        try {
          QueryDetails queryDetails = queryDetailsService.getOneByHiveQueryId(queryId);

          Map<String, Object> jsonObject = new HashMap<>();
          jsonObject.put("query", query);
          jsonObject.put("queryDetails", queryDetails);
          writeData(path, jsonObject);
        }
        catch(Exception e) {
          throw new ArtifactDownloadException("Error trying to fetch query details.", e);
        }
      }
    };

    Artifact dagArtifact = new Artifact() {
      @Override
      public boolean isTemp() {
        return false;
      }

      @Override
      public String getName() {
        return "DAS/DAG." + FILE_EXT;
      }

      @Override
      public void downloadInto(Path path) throws ArtifactDownloadException {
        try {
          DagInfo dagInfo = dagInfoService.getByIdOfHiveQuery(query.getId()).get();
          Map<String, Object> jsonObject = new HashMap<>();
          jsonObject.put("dag", dagInfo);
          writeData(path, jsonObject);
        }
        catch(Exception e) {
          throw new ArtifactDownloadException("Error trying to fetch dag details.", e);
        }
      }
    };

    Artifact vertexArtifact = new Artifact() {
      @Override
      public boolean isTemp() {
        return false;
      }

      @Override
      public String getName() {
        return "DAS/VERTICES." + FILE_EXT;
      }

      @Override
      public void downloadInto(Path path) throws ArtifactDownloadException {
        try {
          DagInfo dagInfo = dagInfoService.getByIdOfHiveQuery(query.getId()).get();
          Collection<VertexInfo> vertexInfos = vertexInfoService.getVerticesByDagId(dagInfo.getDagId());

          Map<String, Object> jsonObject = new HashMap<>();
          jsonObject.put("vertices", vertexInfos);
          writeData(path, jsonObject);
        }
        catch(Exception e) {
          throw new ArtifactDownloadException("Error trying to fetch vertices.", e);
        }
      }
    };

    return ImmutableList.of(queryArtifact, dagArtifact, vertexArtifact);
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path)
    throws ArtifactDownloadException {
    try {

      InputStream stream = Files.newInputStream(path);
      JsonNode node = objectMapper.readTree(stream);
      if (node == null) {
        return;
      }

      if (params.getTezDagId() == null) {
        String dagId = node.path("dag").path("dagId").asText();
        if (dagId != null && !dagId.isEmpty()) {
          params.setTezDagId(dagId);
        }
      }

      if (params.getTezAmAppId() == null) {
        String appId = node.path("dag").path("applicationId").asText();
        if (appId != null && !appId.isEmpty()) {
          params.setTezAmAppId(appId);
        }
      }

      if (params.getAppType() == null) {
        String appType = node.path("query").path("executionMode").asText();
        if (appType != null && !appType.isEmpty()) {
          params.setAppType(appType);
        }
      }

    } catch (IOException e) {
      throw new ArtifactDownloadException(e);
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getHiveQueryId() != null;
  }

}
