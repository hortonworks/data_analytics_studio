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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.debugBundler.BundlerService;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import com.hortonworks.hivestudio.query.services.HiveQueryService;

/**
 * Resource to get the search information for the database
 */
@Path("data-bundle/query")
public class BundleResource {

  private final BundlerService bundlerService;
  private final HiveQueryService hiveQueryService;
  private static final AtomicLong counter = new AtomicLong();

  @Inject
  public BundleResource(BundlerService bundlerService, HiveQueryService hiveQueryService) {
    this.bundlerService = bundlerService;
    this.hiveQueryService = hiveQueryService;
  }

  private boolean userCheck(String queryID, RequestContext requestContext) {
    String userName = requestContext.getUsername();
    if (userName == null || userName.isEmpty()) {
      return false;
    }
    if (requestContext.getRole() == AppAuthentication.Role.ADMIN) {
      return true;
    }

    HiveQuery hiveQuery = hiveQueryService.getOneByHiveQueryId(queryID);
    return userName.equals(hiveQuery.getRequestUser());
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(@PathParam("id") String queryID,
    @QueryParam("action") final String action,
    @Context RequestContext requestContext) throws IOException {

    String userName = requestContext.getUsername();
    boolean actionSucceeded = true;
    File file;

    // TODO: Investigate how the user check can be improved. UserGroupInformation.isSecurityEnabled!?
    if (!userCheck(queryID, requestContext)) {
      return Response.status(Response.Status.UNAUTHORIZED).build();
    }

    if (action != null) {
      switch (action) {
        case "create":
          actionSucceeded = bundlerService.createBundle(queryID, userName);
          break;
        case "delete":
          actionSucceeded = bundlerService.deleteBundle(queryID, userName);
          break;
        case "download":
          file = bundlerService.getBundleFile(queryID, userName);
          if (file.exists()) {
            return Response.ok(file, MediaType.APPLICATION_OCTET_STREAM)
              .header("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"" )
              .build();
          }
          actionSucceeded = false;
          break;
        case "sync-download":
          String fileSuffix = String.format("%s-%d", userName, counter.incrementAndGet());
          String fileName = bundlerService.constructFileName(queryID, fileSuffix);
          file = bundlerService.startBundling(queryID, userName, fileName, true);

          if (file != null && file.exists()) {
            return Response.ok((StreamingOutput) output -> {
              Files.copy(file.toPath(), output);
              file.delete();
            }, MediaType.APPLICATION_OCTET_STREAM)
              .header("Content-Length", file.length())
              .header("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"" )
              .build();
          }
          actionSucceeded = false;
          break;
      }
    }

    if (actionSucceeded) {
      return Response.ok(Collections.singletonMap(
          "bundle", bundlerService.getBundleDetails(queryID, userName))).build();
    } else {
      // TODO: Improve error reporting
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

}
