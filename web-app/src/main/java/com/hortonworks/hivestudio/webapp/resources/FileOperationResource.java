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

import com.hortonworks.hivestudio.common.RESTUtils;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.hdfs.FileOperationService;
import com.hortonworks.hivestudio.common.hdfs.FileResource;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;

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
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;

/**
 * File access resource
 * API:
 * GET /:path
 *      read entire file
 * POST /
 *      create new file
 *      Required: filePath
 *      file should not already exists
 * PUT /:path
 *      update file content
 */
@Slf4j
@Path("files")
public class FileOperationResource {
  public static final String FAKE_FILE = "fakefile://";
  public static final String JSON_PATH_FILE = "jsonpath:";

  private ResourceUtils resourceUtils;
  private RESTUtils restUtils;
  private FileOperationService fileOperationService;

  @Inject
  public FileOperationResource(FileOperationService fileOperationService, ResourceUtils resourceUtils, RESTUtils restUtils){
    this.fileOperationService = fileOperationService;
    this.resourceUtils = resourceUtils;
    this.restUtils = restUtils;
  }
  /**
   * Get single item
   */
  @GET
  @Path("{filePath:.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getFilePage(@PathParam("filePath") String filePath, @QueryParam("page") Long page, @Context RequestContext requestContext) throws IOException, InterruptedException {

    log.debug("Reading file " + filePath);
    try {
      FileResource file = getFileData(filePath, page, requestContext);
      JSONObject object = new JSONObject();
      object.put("file", file);
      return Response.ok(object).status(200).build();
    } catch (WebApplicationException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  private com.hortonworks.hivestudio.common.hdfs.FileResource getFileData(String filePath, Long page, RequestContext requestContext) throws IOException, InterruptedException {
    if (page == null)
      page = 0L;
    com.hortonworks.hivestudio.common.hdfs.FileResource file = new com.hortonworks.hivestudio.common.hdfs.FileResource();

    if (filePath.startsWith(FAKE_FILE)) {
      if (page > 1)
        throw new IllegalArgumentException("There's only one page in fake files");

      String encodedContent = filePath.substring(FAKE_FILE.length());
      String content = new String(Base64.decodeBase64(encodedContent));

      fillFakeFileObject(filePath, file, content);
    } else if (filePath.startsWith(JSON_PATH_FILE)) {
      if (page > 1)
        throw new IllegalArgumentException("There's only one page in fake files");

      String content = getJsonPathContentByUrl(filePath);
      fillFakeFileObject(filePath, file, content);
    } else  {
      filePath = sanitizeFilePath(filePath);
      file = fileOperationService.readFile(filePath, page, resourceUtils.createHdfsContext(requestContext));
    }

    return file;
  }

  protected String getJsonPathContentByUrl(String filePath) throws IOException {
    URL url = new URL(filePath.substring(JSON_PATH_FILE.length()));

    InputStream responseInputStream = restUtils.get(url.toString(), new HashMap<>());
    String response = IOUtils.toString(responseInputStream);

    for (String ref : url.getRef().split("!")) {
      response = JsonPath.read(response, ref);
    }
    return response;
  }


  public void fillFakeFileObject(String filePath, com.hortonworks.hivestudio.common.hdfs.FileResource file, String content) {
    file.setFilePath(filePath);
    file.setFileContent(content);
    file.setHasNext(false);
    file.setPage(0);
    file.setPageCount(1);
  }

  /**
   * Delete single item
   */
  @DELETE
  @Path("{filePath:.*}")
  public Response deleteFile(@PathParam("filePath") String filePath, @Context RequestContext requestContext) throws IOException, InterruptedException {
    filePath = sanitizeFilePath(filePath);
    try {
      boolean removed = fileOperationService.deleteFile(filePath, resourceUtils.createHdfsContext(requestContext));
      if (removed) {
        return Response.status(204).build();
      }
      throw new ItemNotFoundException("FileSystem.delete returned false");
    } catch (WebApplicationException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Update item
   */
  @PUT
  @Path("{filePath:.*}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateFile(FileResourceRequest request,
                             @PathParam("filePath") String filePath, @Context RequestContext requestContext) throws IOException, InterruptedException {
    try {
      filePath = sanitizeFilePath(filePath);
      fileOperationService.updateFile(request.file, filePath, resourceUtils.createHdfsContext(requestContext));
      return Response.status(204).build();
    } catch (WebApplicationException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }


  /**
   * Create script
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createFile(FileResourceRequest request,
                             @Context HttpServletResponse response, @Context UriInfo ui, @Context RequestContext requestContext)
      throws IOException, InterruptedException {
    try {
      fileOperationService.createFile(request.file, resourceUtils.createHdfsContext(requestContext));
      response.setHeader("Location",
          String.format("%s/%s", ui.getAbsolutePath().toString(), request.file.getFilePath()));
      return Response.status(204).build();
    } catch (WebApplicationException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }


//  /**
//   * Checks connection to HDFS
//   * @param context View Context
//   */
//  public static void hdfsSmokeTest(ViewContext context) {
//    try {
//      HdfsApi api;
//      if(props.isPresent()){
//        api = HdfsUtil.connectToHDFSApi(context, props.get());
//      }else{
//        api = HdfsUtil.connectToHDFSApi(context);
//      }
//
//      api.getStatus();
//    } catch (WebApplicationException ex) {
//      throw ex;
//    } catch (Exception ex) {
//      throw new ServiceFormattedException(ex.getMessage(), ex);
//    }
//  }
//
//  /**
//   * Checks connection to User HomeDirectory
//   * @param context View Context
//   */
//  public static void userhomeSmokeTest(ViewContext context) {
//    try {
//      UserResource userservice = new UserResource(context, getViewConfigs(context));
//      userservice.homeDir();
//    } catch (WebApplicationException ex) {
//      throw ex;
//    } catch (Exception ex) {
//      throw new ServiceFormattedException(ex.getMessage(), ex);
//    }
//  }

  /**
   * Wrapper object for json mapping
   */
  public static class FileResourceRequest {
    public com.hortonworks.hivestudio.common.hdfs.FileResource file;
  }

  private String sanitizeFilePath(String filePath){
    if (!filePath.startsWith("/") && !filePath.startsWith(".")) {
      filePath = "/" + filePath;  // some servers strip double slashes in URL
    }
    return filePath;
  }
}
