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

import com.google.common.collect.Lists;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.hdfs.FileOperationService;
import com.hortonworks.hivestudio.hive.client.ColumnDescription;
import com.hortonworks.hivestudio.hive.client.NonPersistentCursor;
import com.hortonworks.hivestudio.hive.internal.BackgroundJob;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.resources.jobs.ResultsResponse;
import com.hortonworks.hivestudio.hive.services.JobService;
import com.hortonworks.hivestudio.common.resource.RequestContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

@Slf4j
@Path("jobs")
public class JobResource {
  private final JobService jobService;
  private ResourceUtils resourceUtils;
  private FileOperationService fileOperationService;

  @Inject
  JobResource(JobService jobService, ResourceUtils resourceUtils, FileOperationService fileOperationService) {
    this.jobService = jobService;
    this.resourceUtils = resourceUtils;
    this.fileOperationService = fileOperationService;
  }

  /**
   * Create job
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response create(JobRequest request, @Context HttpServletResponse response,
                         @Context UriInfo ui, @Context RequestContext requestContext) {
    try {
      Job job = request.job;
      Job createdJob = jobService.create(resourceUtils.createHiveContext(requestContext), job);
      JSONObject jobObject = jsonObjectFromJob(createdJob);
      response.setHeader("Location",
          String.format("%s/%s", ui.getAbsolutePath().toString(), job.getId()));
      return Response.ok(jobObject).status(201).build();
    } catch (Throwable ex) {
      log.error("Error occurred while creating job : ", ex);
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Get single item
   */
  @GET
  @Path("{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getOne(@PathParam("jobId") Integer jobId, @Context RequestContext requestContext) {
    Job job = jobService.getOne(resourceUtils.createHiveContext(requestContext), jobId);
    JSONObject jsonJob = jsonObjectFromJob(job);
    return Response.ok(jsonJob).build();
  }

  private JSONObject jsonObjectFromJob(Job job) {
    Map createdJobMap = null;
    try {
      createdJobMap = PropertyUtils.describe(job);
    } catch (Exception e) {
      throw new ServiceFormattedException(e);
    }
    createdJobMap.remove("class"); // no need to show Bean class on client

    JSONObject jobJson = new JSONObject();
    jobJson.put("job", createdJobMap);
    return jobJson;
  }

  /**
   * TODO : what needxs to be done next.
   * Get query for the job
   */
  @GET
  @Path("{jobId}/query")
  @Produces(MediaType.TEXT_PLAIN)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getQuery(@PathParam("jobId") Integer jobId, @Context RequestContext requestContext) throws IOException, InterruptedException {
    Job job = jobService.getOne(resourceUtils.createHiveContext(requestContext), jobId);
    return Response.ok(job.getQuery()).status(200).build();
  }

  /**
   * Get logs for the job
   */
  @GET
  @Path("{jobId}/logs")
  @Produces(MediaType.TEXT_PLAIN)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getLog(@PathParam("jobId") Integer jobId, @Context RequestContext requestContext) throws IOException, InterruptedException {
    Job job = jobService.getOne(resourceUtils.createHiveContext(requestContext), jobId);
    String logFile = job.getLogFile();
    return createStreamingOutput(requestContext, logFile);
  }

  private Response createStreamingOutput(@Context RequestContext requestContext, String file) {
    StreamingOutput streamingOutput = new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        try (InputStream inputStream = fileOperationService.getInputStream(file,
            resourceUtils.createHdfsContext(requestContext))) {

          IOUtils.copy(inputStream, output);
        } catch (InterruptedException e) {
          log.error("Error occurred while reading file {}", file, e);
          throw new WebApplicationException(e);
        }
      }

    };
    return Response.ok(streamingOutput).status(200).build();
  }

  /**
   * Get job results in csv format
   */
  @GET
  @Path("{jobId}/results/csv/{fileName}")
  @Produces("text/csv")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response getResultsCSV(@PathParam("jobId") Integer jobId,
                                @Context HttpServletResponse response,
                                @PathParam("fileName") String fileName,
                                @QueryParam("columns") final String requestedColumns, @Context RequestContext requestContext) {
    final NonPersistentCursor resultSet = jobService.getResultsCursor(resourceUtils.createHiveContext(requestContext), jobId, fileName);


    StreamingOutput stream = os -> {
      Writer writer = new BufferedWriter(new OutputStreamWriter(os));
      CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
      try {

        List<ColumnDescription> descriptions = resultSet.getDescriptions();
        List<String> headers = Lists.newArrayList();
        for (ColumnDescription description : descriptions) {
          headers.add(description.getName());
        }

        csvPrinter.printRecord(headers.toArray());

        while (resultSet.hasNext()) {
          csvPrinter.printRecord(resultSet.next().getRow());
          writer.flush();
        }
      } finally {
        writer.close();
      }
    };

    return Response.ok(stream).build();
  }

  /**
   * Get job results in csv format
   */
  @GET
  @Path("{jobId}/results/csv/saveToHDFS")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getResultsToHDFS(@PathParam("jobId") Integer jobId,
                                   @QueryParam("commence") String commence,
                                   @QueryParam("file") final String targetFile,
                                   @QueryParam("stop") final String stop,
                                   @QueryParam("columns") final String requestedColumns,
                                   @Context HttpServletResponse response,
                                   @Context RequestContext requestContext
  ) {
    BackgroundJob backgroudJob = jobService.getResultsToHDFS(jobId, commence, targetFile, stop, resourceUtils.createHiveContext(requestContext));
    return Response.ok(backgroudJob).build();
  }


  @Path("{jobId}/status")
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response fetchJobStatus(@PathParam("jobId") Integer jobId, @Context RequestContext requestContext) {
    String jobStatus = jobService.fetchJobStatus(resourceUtils.createHiveContext(requestContext), jobId);
    log.info("jobStatus : {} for jobId : {}", jobStatus, jobId);
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("jobStatus", jobStatus);
    jsonObject.put("jobId", jobId);
    return Response.ok(jsonObject).build();
  }

  /**
   * Get next results page
   */
  @GET
  @Path("{jobId}/results")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getResults(@PathParam("jobId") final Integer jobId,
                             @QueryParam("first") final String fromBeginning,
                             @QueryParam("count") Integer count,
                             @QueryParam("searchId") String searchId,
//                             @QueryParam("format") String format,
                             @QueryParam("columns") final String requestedColumns,
                             @Context RequestContext requestContext) {
    ResultsResponse results = jobService.getResults(jobId, fromBeginning, count, searchId, requestedColumns, resourceUtils.createHiveContext(requestContext));
    return Response.ok(results).build();
  }

  /**
   * Renew expiration time for results
   */
  @GET
  @Path("{jobId}/results/keepAlive")
  public Response keepAliveResults(@PathParam("jobId") Integer jobId,
                                   @QueryParam("first") String fromBeginning,
                                   @QueryParam("count") Integer count) {
    jobService.keepAliveResults(jobId);
    return Response.ok().build();
  }
//
//  /**
//   * Get progress info
//   */
//  @GET
//  @Path("{jobId}/progress")
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response getProgress(@PathParam("jobId") String jobId) {
//    try {
//      final JobController jobController = getResourceManager().readController(jobId);
//
//      ProgressRetriever.Progress progress = new ProgressRetriever(jobController.getJob(), getSharedObjectsFactory()).
//          getProgress();
//
//      return Response.ok(progress).build();
//    } catch (WebApplicationException ex) {
//      throw ex;
//    } catch (ItemNotFound itemNotFound) {
//      throw new NotFoundFormattedException(itemNotFound.getMessage(), itemNotFound);
//    } catch (Exception ex) {
//      throw new ServiceFormattedException(ex.getMessage(), ex);
//    }
//  }


  /**
   * Wrapper object for json mapping
   */
  public static class JobRequest {
    public Job job;
  }
}
