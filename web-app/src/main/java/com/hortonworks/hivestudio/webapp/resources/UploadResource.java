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
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.fs.FSDataInputStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.json.simple.JSONObject;

import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiSupplier;
import com.hortonworks.hivestudio.common.hdfs.HdfsContext;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hive.internal.generators.CreateTableQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.DeleteTableQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.InsertFromQueryGenerator;
import com.hortonworks.hivestudio.hive.internal.generators.InsertFromQueryInput;
import com.hortonworks.hivestudio.hive.persistence.entities.Job;
import com.hortonworks.hivestudio.hive.services.JobService;
import com.hortonworks.hivestudio.hive.services.uploads.CSVParams;
import com.hortonworks.hivestudio.hive.services.uploads.UploadFromHdfsInput;
import com.hortonworks.hivestudio.hive.services.uploads.UploadService;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.PreviewData;
import com.hortonworks.hivestudio.hive.services.uploads.query.DeleteQueryInput;
import com.hortonworks.hivestudio.common.resource.RequestContext;

import lombok.extern.slf4j.Slf4j;

/**
 * UI driven end points for creation of new hive table and inserting data into it.
 * It uploads a file, parses it partially based on its type, generates preview,
 * creates temporary hive table for storage as CSV and actual hive table,
 * uploads the file again, parses it, create CSV stream and upload to hdfs in temporary table,
 * insert rows from temporary table to actual table, delete temporary table.
 * <p/>
 * API:
 * POST /preview : takes stream, parses it and returns preview rows, headers and column type suggestions
 * POST /createTable : runs hive query to create table in hive
 * POST /upload : takes stream, parses it and converts it into CSV and uploads it to the temporary table
 * POST /insertIntoTable : runs hive query to insert data from temporary table to actual hive table
 * POST /deleteTable : deletes the temporary table
 */
@Slf4j
@Path("upload")
public class UploadResource {
  private final UploadService uploadService;
  private final ResourceUtils resourceUtils;
  private Configuration configuration;
  private HdfsApiSupplier hdfsApiSupplier;
  private JobService jobService;

  @Inject
  public UploadResource(UploadService uploadService, ResourceUtils resourceUtils, Configuration configuration,
                        HdfsApiSupplier hdfsApiSupplier, JobService jobService) {
    this.uploadService = uploadService;
    this.resourceUtils = resourceUtils;
    this.configuration = configuration;
    this.hdfsApiSupplier = hdfsApiSupplier;
    this.jobService = jobService;
  }

  public void validateForPreview(UploadFromHdfsInput input) {
    if (input.getIsFirstRowHeader() == null) {
      input.setIsFirstRowHeader(false);
    }

    if (null == input.getInputFileType()) {
      throw new IllegalArgumentException("inputFileType parameter cannot be null.");
    }
    if (null == input.getHdfsPath()) {
      throw new IllegalArgumentException("hdfsPath parameter cannot be null.");
    }
  }

  @POST
  @Path("previewFromHdfs")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response uploadForPreviewFromHDFS(UploadFromHdfsInput input, @Context RequestContext requestContext) {
    InputStream uploadedInputStream = null;
    try {
      uploadedInputStream = getHDFSFileStream(input.getHdfsPath(), resourceUtils.createHdfsContext(requestContext));
      this.validateForPreview(input);
      CSVParams csvParams = getCsvParams(input.getCsvDelimiter(), input.getCsvQuote(), input.getCsvEscape());
      PreviewData pd = uploadService.generatePreview(input.getIsFirstRowHeader(), input.getInputFileType(), csvParams, uploadedInputStream);
      String tableName = getBasenameFromPath(input.getHdfsPath());
      return createPreviewResponse(pd, input.getIsFirstRowHeader(), tableName);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new ServiceFormattedException(e);
    } finally {
      if (null != uploadedInputStream) {
        try {
          uploadedInputStream.close();
        } catch (IOException e) {
          log.error("Exception occured while closing the HDFS file stream for path " + input.getHdfsPath(), e);
        }
      }
    }
  }

  @PUT
  @Path("preview")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public Response uploadForPreview(
      @FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail,
      @FormDataParam("isFirstRowHeader") Boolean isFirstRowHeader,
      @FormDataParam("inputFileType") String inputFileType,
      @FormDataParam("csvDelimiter") String csvDelimiter,
      @FormDataParam("csvEscape") String csvEscape,
      @FormDataParam("csvQuote") String csvQuote
  ) {
      if (null == inputFileType)
        throw new IllegalArgumentException("inputFileType parameter cannot be null.");

      if (null == isFirstRowHeader)
        isFirstRowHeader = false;

      CSVParams csvParams = getCsvParams(csvDelimiter, csvQuote, csvEscape);

      PreviewData pd = uploadService.generatePreview(isFirstRowHeader, inputFileType, csvParams, uploadedInputStream);
      return createPreviewResponse(pd, isFirstRowHeader, getBasename(fileDetail.getFileName()));
  }

  private CSVParams getCsvParams(String csvDelimiter, String csvQuote, String csvEscape) {
    char csvq = CSVParams.DEFAULT_QUOTE_CHAR;
    char csvd = CSVParams.DEFAULT_DELIMITER_CHAR;
    char csve = CSVParams.DEFAULT_ESCAPE_CHAR;

    if (null != csvDelimiter) {
      char[] csvdArray = csvDelimiter.toCharArray();
      if (csvdArray.length > 0) {
        csvd = csvdArray[0];
      }
    }

    if (null != csvQuote) {
      char[] csvqArray = csvQuote.toCharArray();
      if (csvqArray.length > 0) {
        csvq = csvqArray[0];
      }
    }

    if (null != csvEscape) {
      char[] csveArray = csvEscape.toCharArray();
      if (csveArray.length > 0) {
        csve = csveArray[0];
      }
    }

    return new CSVParams(csvd, csvq, csve);
  }


  @Path("uploadFromHDFS")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response uploadFileFromHdfs(UploadFromHdfsInput input, @Context RequestContext requestContext) {
    // create stream and upload
    InputStream hdfsStream = null;
    try {
      hdfsStream = getHDFSFileStream(input.getHdfsPath(), resourceUtils.createHdfsContext(requestContext));
      CSVParams csvParams = getCsvParams(input.getCsvDelimiter(), input.getCsvQuote(), input.getCsvEscape());
      String path = uploadService.uploadFileFromStream(hdfsStream, input.getIsFirstRowHeader(), input.getInputFileType(), input.getTableName(), input.getDatabaseName(), input.getHeader(), input.isContainsEndlines(), csvParams, resourceUtils.createHiveContext(requestContext));

      JSONObject jo = new JSONObject();
      jo.put("uploadedPath", path);

      return Response.ok(jo).build();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new ServiceFormattedException(e);
    } finally {
      if (null != hdfsStream)
        try {
          hdfsStream.close();
        } catch (IOException e) {
          log.error("Exception occured while closing the HDFS stream for path : " + input.getHdfsPath(), e);
        }
    }
  }

  @Path("upload")
  @PUT
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public Response uploadFile(
      @FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail,
      @FormDataParam("isFirstRowHeader") Boolean isFirstRowHeader,
      @FormDataParam("inputFileType") String inputFileType,   // the format of the file uploaded. CSV/JSON etc.
      @FormDataParam("tableName") String tableName,
      @FormDataParam("databaseName") String databaseName,
      @FormDataParam("header") String header,
      @FormDataParam("containsEndlines") boolean containsEndlines,
      @FormDataParam("csvDelimiter") String csvDelimiter,
      @FormDataParam("csvEscape") String csvEscape,
      @FormDataParam("csvQuote") String csvQuote,
      @Context RequestContext requestContext) {
    try {
      CSVParams csvParams = getCsvParams(csvDelimiter, csvQuote, csvEscape);
      ObjectMapper mapper = new ObjectMapper();
      List<ColumnInfo> columnList = mapper.readValue(header, new TypeReference<List<ColumnInfo>>() {
      });
      String path = uploadService.uploadFileFromStream(uploadedInputStream, isFirstRowHeader, inputFileType, tableName, databaseName, columnList, containsEndlines, csvParams, resourceUtils.createHiveContext(requestContext));

      JSONObject jo = new JSONObject();
      jo.put("uploadedPath", path);
      return Response.ok(jo).build();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new ServiceFormattedException(e);
    }
  }

  @Path("insertIntoTable")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response insertFromTempTable(InsertFromQueryInput input, @Context RequestContext requestContext) {
    try {
      String insertQuery = generateInsertFromQuery(input);
      log.info("insertQuery : {}", insertQuery);
      Job job = this.jobService.createJobWithUserSettings(insertQuery, input.getFromDatabase(), "Insert from " +
          input.getFromDatabase() + "." + input.getFromTable() + " to " +
          input.getToDatabase() + "." + input.getToTable(), Job.REFERRER.INTERNAL.name(), resourceUtils.createHiveContext(requestContext));
      log.info("Job created for insert from temp table : {}", job);
      return Response.ok(job).build();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new ServiceFormattedException(e);
    }
  }


  private String generateCreateQuery(TableMeta ti) {
    CreateTableQueryGenerator createTableQueryGenerator = new CreateTableQueryGenerator(ti);
    Optional<String> query = createTableQueryGenerator.getQuery();
    if (query.isPresent()) {
      return query.get();
    } else {
      throw new ServiceFormattedException("Failed to generate create table query.");
    }
  }

  private String generateInsertFromQuery(InsertFromQueryInput input) {
    InsertFromQueryGenerator queryGenerator = new InsertFromQueryGenerator(input);
    Optional<String> query = queryGenerator.getQuery();
    if (query.isPresent()) {
      return query.get();
    } else {
      throw new ServiceFormattedException("Failed to generate Insert From Query.");
    }
  }

  private String generateDeleteQuery(DeleteQueryInput deleteQueryInput) {
    DeleteTableQueryGenerator deleteQuery = new DeleteTableQueryGenerator(deleteQueryInput.getDatabase(), deleteQueryInput.getTable());
    Optional<String> query = deleteQuery.getQuery();
    if (query.isPresent()) {
      return query.get();
    } else {
      throw new ServiceFormattedException("Failed to generate delete table query.");
    }
  }

  private static String getErrorMessage(WebApplicationException e) {
    if (null != e.getResponse() && null != e.getResponse().getEntity())
      return e.getResponse().getEntity().toString();
    else return e.getMessage();
  }

  private Response createPreviewResponse(PreviewData pd, Boolean isFirstRowHeader, String tableName) {
    Map<String, Object> retData = new HashMap<>();
    retData.put("header", pd.getHeader());
    retData.put("rows", pd.getPreviewRows());
    retData.put("isFirstRowHeader", isFirstRowHeader);
    retData.put("tableName", tableName);

    JSONObject jsonObject = new JSONObject(retData);
    return Response.ok(jsonObject).build();
  }

  private String getBasenameFromPath(String path) {
    String fileName = new File(path).getName();
    return getBasename(fileName);
  }

  private String getBasename(String fileName) {
    int index = fileName.indexOf(".");
    if (index != -1) {
      return fileName.substring(0, index);
    }

    return fileName;
  }

  private InputStream getHDFSFileStream(String path, HdfsContext hdfsContext) throws IOException, InterruptedException {
    FSDataInputStream fsStream = getHdfsApi(hdfsContext).open(path);
    return fsStream;
  }

  private HdfsApi getHdfsApi(HdfsContext hdfsContext) {
    return hdfsApiSupplier.get(hdfsContext).get();
  }
}
