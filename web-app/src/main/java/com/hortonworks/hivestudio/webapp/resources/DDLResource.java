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

import com.google.common.base.Strings;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.repository.TablePartitionInfoRepository;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnStats;
import com.hortonworks.hivestudio.common.dto.DatabaseResponse;
import com.hortonworks.hivestudio.hive.internal.dto.TableStats;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.internal.dto.DatabaseWithTableMeta;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.common.dto.TableResponse;
import com.hortonworks.hivestudio.hive.services.DDLService;
import com.hortonworks.hivestudio.hivetools.recommendations.TableRecommendations;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resource to get the DDL information for the database
 */
@Slf4j
@Path("ddl")
public class DDLResource {

  @Context HttpServletRequest httpServletRequest;

  private final DDLService ddlService;
  private ResourceUtils resourceUtils;

  private final TableRecommendations tableRecommendations;

  private final Provider<TablePartitionInfoRepository> tablePartitionInfoRepositoryProvider;

  @Inject
  public DDLResource(DDLService ddlService, ResourceUtils resourceUtils, TableRecommendations tableRecommendations,
                     Provider<TablePartitionInfoRepository> tablePartitionInfoRepositoryProvider) {
    this.ddlService = ddlService;
    this.resourceUtils = resourceUtils;
    this.tableRecommendations = tableRecommendations;
    this.tablePartitionInfoRepositoryProvider = tablePartitionInfoRepositoryProvider;
  }

  @GET
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatabases(@Context RequestContext requestContext, @QueryParam("fromHive") Boolean fromHive) {
    Set<DatabaseResponse> infos;
    if(null != fromHive && fromHive){
      infos = ddlService.getDatabasesFromHive(resourceUtils.createHiveContext(requestContext));
    }else {
      infos = ddlService.getDatabases(resourceUtils.createHiveContext(requestContext));
    }
    JSONObject response = new JSONObject();
    response.put("databases", infos);
    return Response.ok(response).build();
  }

  @GET
  @Path("databases/{database_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatabase(@PathParam("database_id") String databaseName, @Context RequestContext requestContext) {
    DatabaseResponse database = ddlService.getDatabase(resourceUtils.createHiveContext(requestContext), databaseName);
    JSONObject response = new JSONObject();
    response.put("database", database);
    return Response.ok(response).build();
  }

  @GET
  @Path("databases/{database_id}/fetch_all")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatabaseWithTableMeta(@PathParam("database_id") Integer databaseId, @Context RequestContext requestContext) {
    DatabaseWithTableMeta databaseWithTableMeta = ddlService.getDatabaseWithTableMeta(resourceUtils.createHiveContext(requestContext), databaseId);

    JSONObject response = new JSONObject();
    response.put("databaseWithTableMeta", databaseWithTableMeta);
    return Response.ok(response).build();
  }

  @DELETE
  @Path("databases/{database_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteDatabase(@PathParam("database_id") String databaseName, @Context RequestContext requestContext) {
    Job job = ddlService.deleteDatabase(resourceUtils.createHiveContext(requestContext), databaseName);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @POST
  @Path("databases")
  @Produces(MediaType.APPLICATION_JSON)
  public Response createDatabase(CreateDatabaseRequestWrapper wrapper, @Context RequestContext requestContext) {
    String databaseName = wrapper.database.name;
    Job job = ddlService.createDatabase(resourceUtils.createHiveContext(requestContext), databaseName);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @GET
  @Path("databases/{database_id}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTables(@PathParam("database_id") String databaseName, @Context RequestContext requestContext) {
    Set<TableResponse> tables = ddlService.getTables(resourceUtils.createHiveContext(requestContext), databaseName);
    JSONObject response = new JSONObject();
    response.put("tables", tables);
    return Response.ok(response).build();
  }

  @POST
  @Path("databases/{database_id}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createTable(@PathParam("database_id") String databaseName, TableMetaRequest request, @Context RequestContext requestContext) {
    Job job = ddlService.createTable(resourceUtils.createHiveContext(requestContext), databaseName, request.tableInfo);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @PUT
  @Path("databases/{database_id}/tables/{table_id}/rename")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response renameTable(@PathParam("database_id") String oldDatabaseName, @PathParam("table_id") String oldTableName,
                              TableRenameRequest request, @Context RequestContext requestContext) {
    Job job = ddlService.renameTable(resourceUtils.createHiveContext(requestContext), oldDatabaseName, oldTableName, request.newDatabase, request.newTable);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @PUT
  @Path("databases/{database_id}/tables/{table_id}/analyze")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response analyzeTable(@PathParam("database_id") String databaseName, @PathParam("table_id") String tableName,
                               @QueryParam("analyze_columns") String analyzeColumns, @Context RequestContext requestContext) {
    Boolean shouldAnalyzeColumns = Boolean.FALSE;
    if (!Strings.isNullOrEmpty(analyzeColumns)) {
      shouldAnalyzeColumns = Boolean.valueOf(analyzeColumns.trim());
    }
    Job job = ddlService.analyzeTable(resourceUtils.createHiveContext(requestContext), databaseName, tableName, shouldAnalyzeColumns);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @POST
  @Path("databases/{database_id}/tables/ddl")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response generateDDL(TableMetaRequest request, @QueryParam("query_type") String queryType, @Context RequestContext requestContext) {
    String query = ddlService.generateDDL(resourceUtils.createHiveContext(requestContext), request.tableInfo, queryType);
    JSONObject response = new JSONObject();
    response.put("ddl", new DDL(query));
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @GET
  @Path("databases/{database_id}/tables/{table_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getTable(@PathParam("database_id") String databaseName, @PathParam("table_id") String tableName, @Context RequestContext requestContext) {
    TableResponse table = ddlService.getTable(resourceUtils.createHiveContext(requestContext), databaseName, tableName);
    JSONObject response = new JSONObject();
    response.put("table", table);
    return Response.ok(response).build();
  }

  @GET
  @Path("databases/{database_id}/tables/{table_id}/fetch_stats")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response describeTable(@PathParam("database_id") String databaseName, @PathParam("table_id") String tableName, @Context RequestContext requestContext) {
    TableStats tableStats = ddlService.getTableStats(resourceUtils.createHiveContext(requestContext), databaseName, tableName);
    JSONObject response = new JSONObject();
    response.put("tableStats", tableStats);
    return Response.ok(response).build();
  }

  /**
   * @param databaseName
   * @param oldTableName     : this is required in case if the name of table itself is changed in tableMeta
   * @param tableMetaRequest
   * @return
   */
  @PUT
  @Path("databases/{database_id}/tables/{table_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response alterTable(@PathParam("database_id") String databaseName, @PathParam("table_id") String oldTableName, TableMetaRequest tableMetaRequest, @Context RequestContext requestContext) {
    Job job = ddlService.alterTable(resourceUtils.createHiveContext(requestContext), databaseName, oldTableName, tableMetaRequest.tableInfo);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @DELETE
  @Path("databases/{database_id}/tables/{table_id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response deleteTable(@PathParam("database_id") String databaseName, @PathParam("table_id") String tableName, @Context RequestContext requestContext) {
    Job job = ddlService.deleteTable(resourceUtils.createHiveContext(requestContext), databaseName, tableName);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @GET
  @Path("databases/{database_id}/tables/{table_id}/info")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTableInfo(@PathParam("database_id") String databaseName, @PathParam("table_id") String tableName, @Context RequestContext requestContext) {
    TableMeta meta = ddlService.getTableInfo(resourceUtils.createHiveContext(requestContext), databaseName, tableName);
    JSONObject response = new JSONObject();
    response.put("tableInfo", meta);
    return Response.ok(response).build();
  }

  @GET
  @Path("databases/{database_id}/tables/{table_id}/column/{column_id}/stats")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getColumnStats(@PathParam("database_id") String databaseName, @PathParam("table_id") String tableName,
                                 @PathParam("column_id") String columnName, @Context RequestContext requestContext) {
    Job job = ddlService.getColumnStats(resourceUtils.createHiveContext(requestContext), databaseName, tableName, columnName);
    JSONObject response = new JSONObject();
    response.put("job", job);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  @GET
  @Path("databases/{database_id}/tables/{table_id}/column/{column_id}/fetch_stats")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response fetchColumnStats(@PathParam("database_id") String databaseName, @PathParam("table_id") String
      tablename, @PathParam("column_id") String columnName, @QueryParam("job_id") Integer jobId, @Context RequestContext requestContext) {
    ColumnStats columnStats = ddlService.fetchColumnStats(resourceUtils.createHiveContext(requestContext), databaseName, tablename, columnName, jobId);
    columnStats.setTableName(tablename);
    columnStats.setDatabaseName(databaseName);
    JSONObject response = new JSONObject();
    response.put("columnStats", columnStats);
    return Response.status(Response.Status.ACCEPTED).entity(response).build();
  }

  /**
   * Gets recommendations for a table
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("databases/{database_id}/tables/{table_id}/recommendation")
  public Response getTableRecommendations(@PathParam("database_id") String databaseName, @PathParam("table_id") String tableName,
                                          @Context RequestContext context) throws Exception {
    if (StringUtils.isEmpty(databaseName) || StringUtils.isEmpty(tableName)) {
      throw new ServiceFormattedException("Query parameter 'databaseName' & 'tableName' is required.", Response.Status.BAD_REQUEST.getStatusCode());
    }

    TableMeta meta = null;
    try {
      meta = ddlService.getTableInfo(resourceUtils.createHiveContext(context), databaseName, tableName);
    }
    catch(Exception e) {
      throw new ItemNotFoundException("Table with name " + tableName + " not found in database with name " + databaseName);
    }

    Collection<TablePartitionInfo> partitions = tablePartitionInfoRepositoryProvider.get().getAllForTableNotDropped(Integer.parseInt(meta.getId()));

    JSONObject response = new JSONObject();
    response.put("recommendations", tableRecommendations.getRecommendations(meta, partitions));
    return Response.ok(response).status(Response.Status.OK).build();
  }

  @Value
  public static class DDL {
    String query;
  }

  /**
   * Wrapper class for table meta request
   */
  @Value
  public static class TableMetaRequest {
    public TableMeta tableInfo;
  }

  /**
   * Wrapper class for create database request
   */
  @Value
  public static class CreateDatabaseRequestWrapper {
    public CreateDatabaseRequest database;
  }

  /**
   * Request class for create database
   */
  @Value
  public static class CreateDatabaseRequest {
    public String name;
  }

  /**
   * Wrapper class for table rename request
   */
  @Value
  public static class TableRenameRequest {
    /* New database name */
    public String newDatabase;

    /* New table name */
    public String newTable;
  }
}
