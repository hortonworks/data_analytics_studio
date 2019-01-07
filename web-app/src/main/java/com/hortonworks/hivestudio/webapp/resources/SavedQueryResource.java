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

import java.util.Collection;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.json.simple.JSONObject;
import com.hortonworks.hivestudio.hive.persistence.entities.SavedQuery;
import com.hortonworks.hivestudio.hive.services.SavedQueryService;
import com.hortonworks.hivestudio.common.resource.RequestContext;

import lombok.extern.slf4j.Slf4j;

/**
 * Resource class for working with the hive ide Saved queries
 */
@Slf4j
@Path("/savedQueries")
public class SavedQueryResource {

  private final SavedQueryService savedQueryService;

  @Inject
  public SavedQueryResource(SavedQueryService udfService) {
    this.savedQueryService = udfService;
  }

  /**
   * Gets a Saved query for the current user corresponding to a id
   */
  @GET
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOne(@PathParam("id") Integer id, @Context RequestContext context) {
    SavedQuery savedQuery = savedQueryService.getOne(id, context.getUsername());
    JSONObject response = new JSONObject();
    response.put("savedQuery", savedQuery);
    return Response.ok(response).status(Response.Status.OK).build();
  }

  /**
   * Gets all the saved queries for the current user
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAll(@Context RequestContext context) {
    Collection<SavedQuery> savedQueries = savedQueryService.getAllForUser(context.getUsername());
    JSONObject response = new JSONObject();
    response.put("savedQueries", savedQueries);
    return Response.ok(response).status(Response.Status.OK).build();
  }

  /**
   * Adds a saved querie for the current user
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(SavedQueryService.SavedQueryRequest savedQueryRequest,
                         @Context RequestContext context,
                         @Context HttpServletResponse response,
                         @Context UriInfo uriInfo) {
    SavedQuery savedQuery = savedQueryService.createSavedQuery(savedQueryRequest.getSavedQuery(), context.getUsername());

    response.setHeader("Location",
      String.format("%s/%s", uriInfo.getAbsolutePath().toString(), savedQuery.getId()));

    JSONObject op = new JSONObject();
    op.put("savedQuery", savedQuery);
    return Response.status(Response.Status.CREATED).entity(op).build();
  }

  /**
   * Updates a saved query for the current user
   */
  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response update(@PathParam("id") Integer id, SavedQueryService.SavedQueryRequest savedQueryRequest,
                         @Context RequestContext context,
                         @Context HttpServletResponse response,
                         @Context UriInfo uriInfo) {

    SavedQuery savedQuery = savedQueryService.updateSavedQuery(id, savedQueryRequest.getSavedQuery(), context.getUsername());

    response.setHeader("Location",
      String.format("%s/%s", uriInfo.getAbsolutePath().toString(), savedQuery.getId()));

    JSONObject op = new JSONObject();
    op.put("savedQuery", savedQuery);
    return Response.status(Response.Status.OK).entity(op).build();
  }

  /**
   * Deletes a saved query for the current user
   */
  @DELETE
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(@PathParam("id") Integer id, @Context RequestContext context) {
    savedQueryService.removeSavedQuery(id, context.getUsername());
    return Response.noContent().build();
  }
}
