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

import com.google.common.collect.Iterables;
import com.hortonworks.hivestudio.hive.persistence.entities.SuggestedSearch;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import com.hortonworks.hivestudio.webapp.services.SearchesService;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.Collection;

/**
 * Resource to get the search information for the database
 */
@Slf4j
@Path("suggested-searches")
public class SuggestedSearchesResource {

  private final SearchesService searchesService;

  @Inject
  public SuggestedSearchesResource(SearchesService searchesService) {
    this.searchesService = searchesService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAll(@QueryParam("entityType") final String entityType,
                         @Context RequestContext requestContext) {

    Collection<SuggestedSearch> defaultSearches = searchesService.getAllSuggested(entityType);
    Collection<SuggestedSearch> userSearches = searchesService.getAllSaved(entityType, requestContext.getUsername());

    JSONObject result = new JSONObject();
    result.put("searches", Iterables.concat(defaultSearches, userSearches));

    return Response.ok(result).build();
  }

  @DELETE
  @Path("/{searchId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response delete(@PathParam("searchId") final Integer searchId,
                         @Context RequestContext requestContext) {

    SuggestedSearch search = searchesService.delete(searchId);

    // TODO: Send back better messages
    JSONObject op = new JSONObject();
    op.put("search", search);

    return Response.status(Response.Status.OK).entity(op).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(SearchesService.SearchesRequest searchesRequest,
                         @Context RequestContext context,
                         @Context HttpServletResponse response,
                         @Context UriInfo uriInfo) {

    SuggestedSearch search = searchesService.createSaved(searchesRequest.getSuggestedSearch(), context.getUsername());

    response.setHeader("Location",
        String.format("%s/%s", uriInfo.getAbsolutePath().toString(), search.getId()));

    JSONObject op = new JSONObject();
    op.put("search", search);

    return Response.status(Response.Status.CREATED).entity(op).build();
  }

}
