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

import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.dto.DumpInfo;
import com.hortonworks.hivestudio.common.dto.DumpInfoResponse;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpInfo;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpResponse;
import com.hortonworks.hivestudio.common.entities.DBReplicationEntity;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.hive.services.ReplicationDumpService;
import com.hortonworks.hivestudio.common.resource.RequestContext;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;

@Path(Constants.REPLICATION_DUMP_URL)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ReplicationDumpResource {
  private ReplicationDumpService replicationDumpService;
  private ResourceUtils resourceUtils;

  protected Provider<DBReplicationRepository> dbReplicationRepository;

  @Inject
  public ReplicationDumpResource(ReplicationDumpService replicationDumpService, ResourceUtils resourceUtils,
                                 Provider<DBReplicationRepository> dbReplicationRepository){
    this.replicationDumpService = replicationDumpService;
    this.resourceUtils = resourceUtils;
    this.dbReplicationRepository = dbReplicationRepository;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/info")
  public Response getQueryRecommendations(@Context RequestContext context) throws Exception {
    Collection<DBReplicationEntity> replicationInfos = dbReplicationRepository.get().findAll();
    return Response.ok(Collections.singletonMap("infos", replicationInfos)).build();
  }

  @POST
  @Path(Constants.BOOTSTRAP_DUMP_URL + "/databases/{" + Constants.DATABASE_NAME_PARAM + "}")
  public Response createBootstrapDump(@PathParam(Constants.DATABASE_NAME_PARAM) String databaseName, @Context RequestContext requestContext ){
    DumpInfo bootstrapDump = replicationDumpService.createBootstrapDump(resourceUtils.createHiveContext(requestContext), databaseName);
    return Response.ok(new DumpInfoResponse(bootstrapDump)).build();
  }

  @POST
  @Path(Constants.INCREMENTAL_DUMP_URL + "/databases/{" + Constants.DATABASE_NAME_PARAM + "}")
  public Response createIncrementalDump(@PathParam(Constants.DATABASE_NAME_PARAM) String databaseName, @QueryParam(Constants.LAST_REPLICATION_ID_PARAM)
      Integer lastReplicationId, @Context RequestContext requestContext ){
    DumpInfo incrementalDump = replicationDumpService.createIncrementalDump(
        resourceUtils.createHiveContext(requestContext), databaseName, lastReplicationId);
    return Response.ok(new DumpInfoResponse(incrementalDump)).build();
  }

  @POST
  @Path(Constants.FULL_BOOTSTRAP_DUMP_URL)
  public Response createWarehouseBootstrapDump(@Context RequestContext requestContext ){
    WarehouseDumpInfo bootstrapDump = replicationDumpService.createWarehouseBootstrapDump(resourceUtils.createHiveContext(requestContext));
    return Response.ok(new WarehouseDumpResponse(bootstrapDump)).build();
  }

  @POST
  @Path(Constants.FULL_INCREMENTAL_DUMP_URL)
  public Response createWarehouseIncrementalDump(@QueryParam(Constants.LAST_REPLICATION_ID_PARAM)
      Integer lastReplicationId, @QueryParam(Constants.MAX_NUMBER_OF_EVENTS)
      Integer maxNumberOfEvents, @Context RequestContext requestContext ){
    WarehouseDumpInfo incrementalDump = replicationDumpService.createWarehouseIncrementalDump(
        resourceUtils.createHiveContext(requestContext), lastReplicationId, maxNumberOfEvents);
    return Response.ok(new WarehouseDumpResponse(incrementalDump)).build();
  }
}
