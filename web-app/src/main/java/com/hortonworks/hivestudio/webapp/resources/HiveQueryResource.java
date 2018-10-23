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

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.hortonworks.hivestudio.common.dto.HiveQueryDto;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.common.entities.VertexInfo;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.repository.TablePartitionInfoRepository;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hive.services.DDLProxy;
import com.hortonworks.hivestudio.hivetools.recommendations.QueryRecommendations;
import com.hortonworks.hivestudio.hivetools.recommendations.TableRecommendations;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import com.hortonworks.hivestudio.query.services.DagInfoService;
import com.hortonworks.hivestudio.query.services.HiveQueryService;
import com.hortonworks.hivestudio.query.services.QueryDetailsService;
import com.hortonworks.hivestudio.query.services.VertexInfoService;
import com.hortonworks.hivestudio.common.resource.RequestContext;

import lombok.extern.slf4j.Slf4j;

/**
 * Resource class for working with the hive ide Udfs
 */
@Slf4j
@Path("/hive")
public class HiveQueryResource {

  private final HiveQueryService hiveQueryService;
  private final DagInfoService dagInfoService;
  private final VertexInfoService vertexInfoService;
  private final QueryDetailsService queryDetailsService;
  private final DDLProxy ddlProxy;

  private final Provider<TablePartitionInfoRepository> tablePartitionInfoRepositoryProvider;

  private final QueryRecommendations queryRecommendations;
  private final TableRecommendations tableRecommendations;

  @Inject
  public HiveQueryResource(HiveQueryService hiveQueryService, DagInfoService dagInfoService,
                           VertexInfoService vertexInfoService, QueryDetailsService queryDetailsService,
                           QueryRecommendations queryRecommendations, TableRecommendations tableRecommendations,
                           DDLProxy ddlProxy,
                           Provider<TablePartitionInfoRepository> tablePartitionInfoRepositoryProvider) {
    this.hiveQueryService = hiveQueryService;
    this.dagInfoService = dagInfoService;
    this.vertexInfoService = vertexInfoService;
    this.queryDetailsService = queryDetailsService;
    this.ddlProxy = ddlProxy;


    this.queryRecommendations = queryRecommendations;
    this.tableRecommendations = tableRecommendations;

    this.tablePartitionInfoRepositoryProvider = tablePartitionInfoRepositoryProvider;
  }


  /**
   * Gets single query for the current user
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/query/{id}")
  public Response getOne(@PathParam("id") Long id, @QueryParam("extended") boolean extended, @Context RequestContext context) {
    HiveQuery hiveQuery = hiveQueryService.getOne(id);
    HiveQueryDto query = new HiveQueryDto(hiveQuery);
    Optional<DagInfo> dagInfo = dagInfoService.getByIdOfHiveQuery(hiveQuery.getId());
    if (dagInfo.isPresent()) {
      query.setDagInfo(dagInfo.get());
    }
    if (extended) {
      QueryDetails queryDetails = queryDetailsService.getOneByHiveQueryId(hiveQuery.getQueryId());
      query.setQueryDetails(queryDetails);
    }
    return Response.ok(Collections.singletonMap("query", query)).build();
  }

  /**
   * Gets single query with the given hiveQueryId for the current user
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/query")
  public Response getOneByHiveQueryId(@QueryParam("queryId") String id, @QueryParam("extended") boolean extended, @Context RequestContext context) {
    if (StringUtils.isEmpty(id)) {
      throw new ServiceFormattedException("Query parameter 'queryId' is required.", Response.Status.BAD_REQUEST.getStatusCode());
    }
    HiveQuery hiveQuery = hiveQueryService.getOneByHiveQueryId(id);
    HiveQueryDto query = new HiveQueryDto(hiveQuery);
    Optional<DagInfo> dagInfo = dagInfoService.getByIdOfHiveQuery(hiveQuery.getId());
    if (dagInfo.isPresent()) {
      query.setDagInfo(dagInfo.get());
    }

    if (extended) {
      QueryDetails queryDetails = queryDetailsService.getOneByHiveQueryId(id);
      query.setQueryDetails(queryDetails);
    }
    return Response.ok(Collections.singletonMap("query", query)).build();
  }

  /**
   * Gets recommendations for a specific query
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/query/recommendations")
  public Response getQueryRecommendations(@QueryParam("queryId") String id, @Context RequestContext context) throws Exception {
    if (StringUtils.isEmpty(id)) {
      throw new ServiceFormattedException("Query parameter 'queryId' is required.", Response.Status.BAD_REQUEST.getStatusCode());
    }

    HiveQuery hiveQuery = hiveQueryService.getOneByHiveQueryId(id);
    HiveQueryDto query = new HiveQueryDto(hiveQuery);
    Optional<DagInfo> dagInfo = dagInfoService.getByIdOfHiveQuery(hiveQuery.getId());
    if (dagInfo.isPresent()) {
      query.setDagInfo(dagInfo.get());
    }

    QueryDetails queryDetails = queryDetailsService.getOneByHiveQueryId(id);
    query.setQueryDetails(queryDetails);

    HashSet<Recommendation> allRecommendations = new HashSet<>();

    HashMap<String, ColumnInfo> columnHash = new HashMap<>();
    hiveQuery.getTablesRead().forEach(tableRead -> {
      try {
        TableMeta table = ddlProxy.getTableMetaData(tableRead.get("database").asText(), tableRead.get("table").asText());
        Collection<TablePartitionInfo> partitions = tablePartitionInfoRepositoryProvider.get().getAllForTableNotDropped(Integer.parseInt(table.getId()));

        if(table != null) {
          for (ColumnInfo columnInfo : table.getColumns()) {
            columnHash.put(QueryRecommendations.getColumnKey(table.getDatabase(), table.getTable(), columnInfo.getName()), columnInfo);
          }
        }

        allRecommendations.addAll(tableRecommendations.getRecommendations(table, partitions));
      }
      catch(Exception e) {
        log.warn("Table recommendation generation failed for {}. {}", tableRead.toString(), e);
      }
    });

    try {
      allRecommendations.addAll(queryRecommendations.getRecommendations(hiveQuery, queryDetails, columnHash));
    }
    catch(Exception e) {
      log.warn("Recommendation generation failed for query {}. {}", id, e);
    }

    return Response.ok(Collections.singletonMap("recommendations", allRecommendations)).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/query/download-all")
  public Response downloadAllLogs(@QueryParam("queryId") String id, @Context RequestContext context) {
    if (StringUtils.isEmpty(id)) {
      log.error("Query parameter 'queryId' is required for downloading all logs.");
      throw new ServiceFormattedException("Query parameter 'queryId' is required.", Response.Status.BAD_REQUEST.getStatusCode());
    }
    InputStream input = hiveQueryService.getDownloadAllStream(id);
    StreamingOutput output = out -> IOUtils.copyLarge(input, out);

    //TODO: check implementation
    return Response.ok(output).header("Content-Disposition", "attachment; filename=\"" + id + ".zip\"").build();
  }


  /**
   * Gets a single dag info
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/dag/{id}")
  public Response getOneDags(@PathParam("id")Long id, @QueryParam("extended") boolean extended, @Context RequestContext context) {
    Optional<DagInfo> dagInfoOpt = dagInfoService.getOne(id);
    if (!dagInfoOpt.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    DagInfo dagInfo = dagInfoOpt.get();
    if (extended) {
      QueryDetails details = queryDetailsService.getOneByDagId(dagInfo.getDagId());
      dagInfo.setDetails(details);
    }
    return Response.ok(Collections.singletonMap("dag", dagInfo)).build();
  }

  /**
   * Gets a single dag info by dagId
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/dag")
  public Response getDagByDagId(@QueryParam("dagId") String id, @QueryParam("extended") boolean extended, @Context RequestContext context) {
    if (StringUtils.isEmpty(id)) {
      throw new ServiceFormattedException("Query parameter 'dagId' is required.", Response.Status.BAD_REQUEST.getStatusCode());
    }
    Optional<DagInfo> dagInfo = dagInfoService.getOneByDagId(id);
    if (!dagInfo.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    if (extended) {
      QueryDetails details = queryDetailsService.getOneByDagId(id);
      dagInfo.get().setDetails(details);
    }
    return Response.ok(Collections.singletonMap("dag", dagInfo.get())).build();
  }

  /**
   * Gets a single dag info
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/vertices/{id}")
  public Response getOneVertex(@PathParam("id") Long id, @Context RequestContext context) {
    VertexInfo queries = vertexInfoService.getOne(id);
    return Response.ok(Collections.singletonMap("vertex", queries)).build();
  }

  /**
   * Gets a single dag info by dagId
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/vertices")
  public Response getVertexByVertexId(@QueryParam("vertexId") String id, @QueryParam("dagId") String dagId, @Context RequestContext context) {
    if (StringUtils.isEmpty(id) && StringUtils.isEmpty(dagId)) {
      throw new ServiceFormattedException("Query parameter 'vertexId' or 'dagId' is required.", Response.Status.BAD_REQUEST.getStatusCode());
    }

    Map<String, Object> response = new HashMap<>();
    if (!StringUtils.isEmpty(id)) {
      VertexInfo queries = vertexInfoService.getOneByVertexId(id);
      response.put("vertex", queries);
    } else {
      Collection<VertexInfo> vertices = vertexInfoService.getVerticesByDagId(dagId);
      response.put("vertices", vertices);
    }
    return Response.ok(response).build();
  }

}
