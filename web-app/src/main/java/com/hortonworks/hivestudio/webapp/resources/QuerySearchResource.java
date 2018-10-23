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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.hortonworks.hivestudio.common.AppAuthentication.Role;
import com.hortonworks.hivestudio.common.dto.FacetValue;
import com.hortonworks.hivestudio.common.dto.HiveQueryDto;
import com.hortonworks.hivestudio.common.exception.generic.ConstraintViolationException;
import com.hortonworks.hivestudio.common.repository.PageData;
import com.hortonworks.hivestudio.common.util.MetaInfo;
import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.hive.services.DDLService;
import com.hortonworks.hivestudio.query.dto.FieldInformation;
import com.hortonworks.hivestudio.query.dto.SearchRequest;
import com.hortonworks.hivestudio.query.services.SearchService;
import com.hortonworks.hivestudio.common.resource.RequestContext;

import lombok.Value;

/**
 * Resource class for working with the hive ide Udfs
 */
@Path("/query")
public class QuerySearchResource {

  private final SearchService searchService;
  private final ResourceUtils resourceUtils;
  private final DDLService ddlService;

  @Inject
  public QuerySearchResource(SearchService searchService,
                             ResourceUtils resourceUtils,
                             DDLService ddlService) {
    this.searchService = searchService;
    this.resourceUtils = resourceUtils;
    this.ddlService = ddlService;
  }


  /**
   * Gets a list of query matching the basic search
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/search")
  public Response getBasicSearchedQueries(@QueryParam("text") String queryText,
                                          @QueryParam("sort") String sortText,
                                          @QueryParam("type") String searchType,
                                          @QueryParam("offset") Integer offset,
                                          @QueryParam("limit") Integer limit,
                                          @QueryParam("startTime") Long startTime,
                                          @QueryParam("endTime") Long endTime,
                                          @Context RequestContext context,
                                          @Context UriInfo ui) {
    PageData<HiveQueryDto> pageData = null;
    SearchService.SearchType validatedSearchType =
        SearchService.SearchType.validateAndGetSearchType(searchType);
    switch (validatedSearchType) {
      case ADVANCED:
      case BASIC:
        pageData = searchService.doBasicSearch(queryText, sortText, offset, limit, startTime,
            endTime, new ArrayList<>(), new ArrayList<>(), getEffectiveUser(context));
    }

    MetaInfo metaInfo = MetaInfo.builder().fromPageData(pageData).build();
    Map<String, Object> response = ImmutableMap.of(
        "queries", pageData.getEntities(),
        "meta", metaInfo);
    return Response.ok(response).build();
  }

  /**
   * Gets a list of query matching the basic search
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/search")
  public Response getBasicSearchedQueriesWithFacets(SearchRequest.SearchRequestWrapper request,
                                          @Context RequestContext context,
                                          @Context UriInfo ui) {

    SearchRequest search = request.getSearch();

    PageData<HiveQueryDto> pageData = null;
    SearchService.SearchType validatedSearchType =
        SearchService.SearchType.validateAndGetSearchType(search.getType());
    switch (validatedSearchType) {
      case ADVANCED:
      case BASIC:
        pageData = searchService.doBasicSearch(search.getText(), search.getSortText(),
            search.getOffset(), search.getLimit(), search.getStartTime(), search.getEndTime(),
            search.getFacets(), search.getRangeFacets(), getEffectiveUser(context));
    }

    MetaInfo metaInfo = MetaInfo.builder().fromPageData(pageData).build();
    Map<String, Object> response = ImmutableMap.of(
        "queries", pageData.getEntities(),
        "meta", metaInfo);
    return Response.ok(response).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/fields-information")
  public Response getSearchableColumnInfo(@Context RequestContext context) {
    List<FieldInformation> fieldsInformations = SearchService.getFieldsInformation();
    return Response.ok(Collections.singletonMap("fieldsInfo", fieldsInformations)).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/facets")
  public Response getFacetValues(@QueryParam("text") String queryText,
                                 @QueryParam("facetFields") String facetFields,
                                 @QueryParam("startTime") Long startTime,
                                 @QueryParam("endTime") Long endTime,
                                 @Context RequestContext context) {
    if (StringUtils.isEmpty(facetFields)) {
      throw new ConstraintViolationException("'facetField' query parameter is required.", Response.Status.BAD_REQUEST.getStatusCode());
    }
    Pair<List<FacetValue>, List<FacetValue>> facetsPair = searchService.getFacetValues(
        queryText, facetFields, startTime, endTime, getEffectiveUser(context));
    List<FacetValue> facets = facetsPair.getFirst();
    List<FacetValue> rangeFacets = facetsPair.getSecond();
    Map<String, Object> response = ImmutableMap.of(
        "facets", facets,
        "rangeFacets", rangeFacets);
    return Response.ok(response).build();
  }


  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/kill-query")
  public Response killQuery(KillQueryRequest request, @Context RequestContext context) {
    ddlService.killQueries(request.getQueryIds(), resourceUtils.createHiveContext(context));
    return Response.ok(Collections.emptyMap()).build();
  }

  @Value
  public static class KillQueryRequest {
    private List<String> queryIds;
  }

  private String getEffectiveUser(RequestContext context) {
    if (context.getRole() == Role.ADMIN) {
      return null;
    }
    return context.getUsername();
  }
}
