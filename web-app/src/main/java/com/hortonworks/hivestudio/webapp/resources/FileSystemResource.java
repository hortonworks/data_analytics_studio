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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiException;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiSupplier;
import com.hortonworks.hivestudio.common.hdfs.HdfsService;
import com.hortonworks.hivestudio.common.resource.RequestContext;

import lombok.extern.slf4j.Slf4j;

/**
 * File operations service
 */
@Singleton
@Slf4j
@Consumes(MediaType.APPLICATION_JSON)
@Path("ops")
public class FileSystemResource extends HdfsService {

  private ResourceUtils resourceUtils;

  /**
   * Constructor
   * @param configuration
   * @param resourceUtils
   */
  @Inject
  public FileSystemResource(Configuration configuration, HdfsApiSupplier hdfsApiSupplier, ResourceUtils resourceUtils) {
    super(configuration, hdfsApiSupplier);
    this.resourceUtils = resourceUtils;
  }

  /**
   * List dir
   * @param path path
   * @param requestContext
   * @return response with dir content
   */
  @GET
  @Path("/listdir")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listdir(@QueryParam("path") String path, @Context RequestContext requestContext) {
    try {
      Map<String, Object> response = new HashMap<>();
      response.put("files", getHdfsApi(requestContext).fileStatusToJSON(getHdfsApi(requestContext).listdir(path)));
      response.put("meta", getHdfsApi(requestContext).fileStatusToJSON(getHdfsApi(requestContext).getFileStatus(path)));
      return Response.ok(response).build();
    } catch (FileNotFoundException ex) {
      throw new ItemNotFoundException(ex.getMessage(), ex);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Rename
   * @param request rename request
   * @param requestContext
   * @return response with success
   */
  @POST
  @Path("/rename")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response rename(final SrcDstFileRequest request, @Context RequestContext requestContext) {
    try {
      HdfsApi api = getHdfsApi(requestContext);
      ResponseBuilder result;
      if (api.rename(request.src, request.dst)) {
        result = Response.ok(getHdfsApi(requestContext).fileStatusToJSON(api
            .getFileStatus(request.dst)));
      } else {
        result = Response.ok(new FileOperationResult(false, "Can't move '" + request.src + "' to '" + request.dst + "'")).status(422);
      }
      return result.build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Chmod
   * @param request chmod request
   * @param requestContext
   * @return response with success
   */
  @POST
  @Path("/chmod")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response chmod(final ChmodRequest request,  @Context RequestContext requestContext) {
    try {
      HdfsApi api = getHdfsApi(requestContext);
      ResponseBuilder result;
      if (api.chmod(request.path, request.mode)) {
        result = Response.ok(getHdfsApi(requestContext).fileStatusToJSON(api
            .getFileStatus(request.path)));
      } else {
        result = Response.ok(new FileOperationResult(false, "Can't chmod '" + request.path + "'")).status(422);
      }
      return result.build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Copy file
   * @param request source and destination request
   * @param requestContext
   * @return response with success
   */
  @POST
  @Path("/move")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response move(final MultiSrcDstFileRequest request,
                       @Context HttpHeaders headers, @Context UriInfo ui, @Context RequestContext requestContext) {
    try {
      HdfsApi api = getHdfsApi(requestContext);
      ResponseBuilder result;
      String message = "";

      List<String> sources = request.sourcePaths;
      String destination = request.destinationPath;
      if(sources.isEmpty()) {
        result = Response.ok(new FileOperationResult(false, "Can't move 0 file/folder to '" + destination + "'")).
          status(422);
        return result.build();
      }

      int index = 0;
      for (String src : sources) {
        String fileName = getFileName(src);
        String finalDestination = getDestination(destination, fileName);
        try {
          if (api.rename(src, finalDestination)) {
            index ++;
          } else {
            message = "Failed to move '" + src + "' to '" + finalDestination + "'";
            break;
          }
        } catch (IOException exception) {
          message = exception.getMessage();
          log.error("Failed to move '{}' to '{}'. Exception: {}", src, finalDestination,
            exception.getMessage());
          break;
        }
      }
      if (index == sources.size()) {
        result = Response.ok(new FileOperationResult(true)).status(200);
      } else {
        FileOperationResult errorResult = getFailureFileOperationResult(sources, index, message);
        result = Response.ok(errorResult).status(422);
      }
      return result.build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Copy file
   * @param request source and destination request
   * @param requestContext
   * @return response with success
   */
  @POST
  @Path("/copy")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response copy(final MultiSrcDstFileRequest request,
                       @Context HttpHeaders headers, @Context UriInfo ui, @Context RequestContext requestContext) {
    try {
      HdfsApi api = getHdfsApi(requestContext);
      ResponseBuilder result;
      String message = "";

      List<String> sources = request.sourcePaths;
      String destination = request.destinationPath;
      if(sources.isEmpty()) {
        result = Response.ok(new FileOperationResult(false, "Can't copy 0 file/folder to '" + destination + "'")).
          status(422);
        return result.build();
      }

      int index = 0;
      for (String src : sources) {
        String fileName = getFileName(src);
        String finalDestination = getDestination(destination, fileName);
        try {
          api.copy(src, finalDestination);
          index ++;
        } catch (IOException|HdfsApiException exception) {
          message = exception.getMessage();
          log.error("Failed to copy '{}' to '{}'. Exception: {}", src, finalDestination,
            exception.getMessage());
          break;
        }
      }
      if (index == sources.size()) {
        result = Response.ok(new FileOperationResult(true)).status(200);
      } else {
        FileOperationResult errorResult = getFailureFileOperationResult(sources, index, message);
        result = Response.ok(errorResult).status(422);
      }
      return result.build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Make directory
   * @param request make directory request
   * @param requestContext
   * @return response with success
   */
  @PUT
  @Path("/mkdir")
  @Produces(MediaType.APPLICATION_JSON)
  public Response mkdir(final MkdirRequest request, @Context RequestContext requestContext) {
    try{
      HdfsApi api = getHdfsApi(requestContext);
      ResponseBuilder result;
      if (api.mkdir(request.path)) {
        result = Response.ok(getHdfsApi(requestContext).fileStatusToJSON(api.getFileStatus(request.path)));
      } else {
        result = Response.ok(new FileOperationResult(false, "Can't create dir '" + request.path + "'")).status(422);
      }
      return result.build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Empty trash
   * @return response with success
   * @param requestContext
   */
  @DELETE
  @Path("/trash/emptyTrash")
  @Produces(MediaType.APPLICATION_JSON)
  public Response emptyTrash(@Context RequestContext requestContext) {
    try {
      HdfsApi api = getHdfsApi(requestContext);
      api.emptyTrash();
      return Response.ok(new FileOperationResult(true)).build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Move to trash
   * @param request remove request
   * @param requestContext
   * @return response with success
   */
  @DELETE
  @Path("/moveToTrash")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response moveToTrash(MultiRemoveRequest request, @Context RequestContext requestContext) {
    try {
      ResponseBuilder result;
      HdfsApi api = getHdfsApi(requestContext);
      String trash = api.getTrashDirPath();
      String message = "";

      if (request.paths.size() == 0) {
        result = Response.ok(new FileOperationResult(false, "No path entries provided.")).status(422);
      } else {
        if (!api.exists(trash)) {
          if (!api.mkdir(trash)) {
            result = Response.ok(new FileOperationResult(false, "Trash dir does not exists. Can't create dir for " +
              "trash '" + trash + "'")).status(422);
            return result.build();
          }
        }

        int index = 0;
        for (MultiRemoveRequest.PathEntry entry : request.paths) {
          String trashFilePath = api.getTrashDirPath(entry.path);
          try {
            if (api.rename(entry.path, trashFilePath)) {
              index ++;
            } else {
              message = "Failed to move '" + entry.path + "' to '" + trashFilePath + "'";
              break;
            }
          } catch (IOException exception) {
            message = exception.getMessage();
            log.error("Failed to move '{}' to '{}'. Exception: {}", entry.path, trashFilePath,
              exception.getMessage());
            break;
          }
        }
        if (index == request.paths.size()) {
          result = Response.ok(new FileOperationResult(true)).status(200);
        } else {
          FileOperationResult errorResult = getFailureFileOperationResult(getPathsFromPathsEntries(request.paths), index, message);
          result = Response.ok(errorResult).status(422);
        }
      }
      return result.build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Remove
   * @param request remove request
   * @param requestContext
   * @return response with success
   */
  @DELETE
  @Path("/remove")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response remove(MultiRemoveRequest request, @Context HttpHeaders headers,
                         @Context UriInfo ui,@Context RequestContext requestContext) {
    try {
      HdfsApi api = getHdfsApi(requestContext);
      ResponseBuilder result;
      String message = "";
      if(request.paths.size() == 0) {
        result = Response.ok(new FileOperationResult(false, "No path entries provided."));
      } else {
        int index = 0;
        for (MultiRemoveRequest.PathEntry entry : request.paths) {
          try {
            if (api.delete(entry.path, entry.recursive)) {
              index++;
            } else {
              message = "Failed to remove '" + entry.path + "'";
              break;
            }
          } catch (IOException exception) {
            message = exception.getMessage();
            log.error("Failed to remove '{}'. Exception: {}", entry.path, exception.getMessage());
            break;
          }

        }
        if (index == request.paths.size()) {
          result = Response.ok(new FileOperationResult(true)).status(200);
        } else {
          FileOperationResult errorResult = getFailureFileOperationResult(getPathsFromPathsEntries(request.paths), index, message);
          result = Response.ok(errorResult).status(422);
        }
      }
      return result.build();
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  private List<String> getPathsFromPathsEntries(List<MultiRemoveRequest.PathEntry> paths) {
    List<String> entries = new ArrayList<>();
    for(MultiRemoveRequest.PathEntry path: paths) {
      entries.add(path.path);
    }
    return entries;
  }

  private FileOperationResult getFailureFileOperationResult(List<String> paths, int failedIndex, String message) {
    List<String> succeeded = new ArrayList<>();
    List<String> unprocessed = new ArrayList<>();
    List<String> failed = new ArrayList<>();
    ListIterator<String> iter = paths.listIterator();
    while (iter.hasNext()) {
      int index = iter.nextIndex();
      String path = iter.next();
      if (index < failedIndex) {
        succeeded.add(path);
      } else if (index == failedIndex) {
        failed.add(path);
      } else {
        unprocessed.add(path);
      }
    }
    return new FileOperationResult(false, message, succeeded, failed, unprocessed);
  }

  private String getDestination(String baseDestination, String fileName) {
    if(baseDestination.endsWith("/")) {
      return baseDestination + fileName;
    } else {
      return baseDestination + "/" + fileName;
    }
  }

  private String getFileName(String srcPath) {
    return srcPath.substring(srcPath.lastIndexOf('/') + 1);
  }

  /**
   * Wrapper for json mapping of mkdir request
   */
  @XmlRootElement
  public static class MkdirRequest {
    @XmlElement(nillable = false, required = true)
    public String path;
  }

  /**
   * Wrapper for json mapping of chmod request
   */
  @XmlRootElement
  public static class ChmodRequest {
    @XmlElement(nillable = false, required = true)
    public String path;
    @XmlElement(nillable = false, required = true)
    public String mode;
  }

  /**
   * Wrapper for json mapping of request with
   * source and destination
   */
  @XmlRootElement
  public static class SrcDstFileRequest {
    @XmlElement(nillable = false, required = true)
    public String src;
    @XmlElement(nillable = false, required = true)
    public String dst;
  }

  /**
   * Wrapper for json mapping of request with multiple
   * source and destination
   */
  @XmlRootElement
  public static class MultiSrcDstFileRequest {
    @XmlElement(nillable = false, required = true)
    public List<String> sourcePaths = new ArrayList<>();
    @XmlElement(nillable = false, required = true)
    public String destinationPath;
  }

  /**
   * Wrapper for json mapping of remove request
   */
  @XmlRootElement
  public static class MultiRemoveRequest {
    @XmlElement(nillable = false, required = true)
    public List<PathEntry> paths = new ArrayList<>();
    public static class PathEntry {
      @XmlElement(nillable = false, required = true)
      public String path;
      public boolean recursive;
    }
  }

  private HdfsApi getHdfsApi(RequestContext requestContext){
    return hdfsApiSupplier.get(resourceUtils.createHdfsContext(requestContext)).get();
  }
}
