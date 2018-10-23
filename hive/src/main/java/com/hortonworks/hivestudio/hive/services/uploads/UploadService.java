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
package com.hortonworks.hivestudio.hive.services.uploads;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;

import com.hortonworks.hivestudio.common.config.AuthConfig;
import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.hdfs.HdfsContext;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiSupplier;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.HiveUtils;
import com.hortonworks.hivestudio.hive.client.DDLDelegator;
import com.hortonworks.hivestudio.hive.client.DDLDelegatorImpl;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.services.ConnectionFactory;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.DataParser;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.ParseOptions;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.PreviewData;

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
public class UploadService {
  private Configuration configuration;
  private ConnectionSystem connectionSystem;
  private HdfsApiSupplier hdfsApiSupplier;
  private HiveUtils hiveUtils;
  private ConnectionFactory connectionFactory;
  private HiveConfiguration hiveConfiguration;
  private final AuthConfig authConfig;

  @Inject
  UploadService(Configuration configuration, ConnectionSystem connectionSystem, HdfsApiSupplier hdfsApiSupplier,
                HiveUtils hiveUtils, ConnectionFactory connectionFactory, HiveConfiguration hiveConfiguration, AuthConfig authConfig) {
    this.configuration = configuration;
    this.connectionSystem = connectionSystem;
    this.hdfsApiSupplier = hdfsApiSupplier;
    this.hiveUtils = hiveUtils;
    this.connectionFactory = connectionFactory;
    this.hiveConfiguration = hiveConfiguration;
    this.authConfig = authConfig;
  }

  public String getHiveMetaStoreLocation(String db, String table, HiveContext hiveContext) {
    String locationColValue = "Location:";
    String urlString = null;
    DDLDelegator delegator = new DDLDelegatorImpl(hiveContext, configuration, connectionSystem.getActorSystem(), connectionSystem.getOperationController(hiveContext));

    List<Row> result = delegator.getTableDescriptionFormatted(connectionFactory.create(hiveContext), db, table);
    for (Row row : result) {
      if (row != null && row.getRow().length > 1 && row.getRow()[0] != null && row.getRow()[0].toString().trim().equals(locationColValue)) {
        urlString = row.getRow()[1] == null ? null : row.getRow()[1].toString();
        break;
      }
    }

    String tablePath = null;
    if (null != urlString) {
      try {
        URI uri = new URI(urlString);
        tablePath = uri.getPath();
      } catch (URISyntaxException e) {
        log.debug("Error occurred while parsing as url : ", urlString, e);
      }
    } else {
      String basePath = getHiveMetaStoreLocation();
      if (!basePath.endsWith("/")) {
        basePath = basePath + "/";
      }
      if (db != null && !db.equals(Constants.HIVE_DEFAULT_DB)) {
        basePath = basePath + db + ".db/";
      }
      tablePath = basePath + table;
    }

    return tablePath + "/" + table;
  }

  private String getHiveMetaStoreLocation() {
    return configuration.get(Constants.HIVE_METASTORE_LOCATION_KEY_VIEW_PROPERTY, Constants.HIVE_DEFAULT_METASTORE_LOCATION);
  }

  void uploadFile(final String filePath, InputStream inputStream, HdfsApi hdfsApi)
      throws IOException, InterruptedException {
    try (FSDataOutputStream outputStream = hdfsApi.create(filePath, false)) {
      IOUtils.copy(inputStream, outputStream);
    }
  }

  private static String getErrorMessage(WebApplicationException e) {
    if (null != e.getResponse() && null != e.getResponse().getEntity())
      return e.getResponse().getEntity().toString();
    else return e.getMessage();
  }

  public PreviewData generatePreview(Boolean isFirstRowHeader, String inputFileType, CSVParams csvParams, InputStream uploadedInputStream) {
    ParseOptions parseOptions = new ParseOptions();
    parseOptions.setOption(ParseOptions.OPTIONS_FILE_TYPE, inputFileType);
    if (inputFileType.equals(ParseOptions.InputFileType.CSV.toString())) {
      if (isFirstRowHeader)
        parseOptions.setOption(ParseOptions.OPTIONS_HEADER, ParseOptions.HEADER.FIRST_RECORD.toString());
      else
        parseOptions.setOption(ParseOptions.OPTIONS_HEADER, ParseOptions.HEADER.NONE.toString());

      parseOptions.setOption(ParseOptions.OPTIONS_CSV_DELIMITER, csvParams.getCsvDelimiter());
      parseOptions.setOption(ParseOptions.OPTIONS_CSV_ESCAPE_CHAR, csvParams.getCsvEscape());
      parseOptions.setOption(ParseOptions.OPTIONS_CSV_QUOTE, csvParams.getCsvQuote());
    } else
      parseOptions.setOption(ParseOptions.OPTIONS_HEADER, ParseOptions.HEADER.EMBEDDED.toString());

    log.info("isFirstRowHeader : {}, inputFileType : {}", isFirstRowHeader, inputFileType);


    try {
      Reader reader = getInputStreamReader(uploadedInputStream);
      try (DataParser dataParser = new DataParser(reader, parseOptions)) {;
        return dataParser.parsePreview();
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new ServiceFormattedException(e);
    }
  }

  public String uploadFileFromStream(
      InputStream uploadedInputStream,
      Boolean isFirstRowHeader,
      String inputFileType,   // the format of the file uploaded. CSV/JSON etc.
      String tableName,
      String databaseName,
      List<ColumnInfo> header,
      boolean containsEndlines,
      CSVParams csvParams,
      HiveContext hiveContext) throws Exception {
    log.info(" uploading file into databaseName {}, tableName {}", databaseName, tableName);
    ParseOptions parseOptions = new ParseOptions();
    parseOptions.setOption(ParseOptions.OPTIONS_FILE_TYPE, inputFileType);
    if (isFirstRowHeader) {
      parseOptions.setOption(ParseOptions.OPTIONS_HEADER, ParseOptions.HEADER.FIRST_RECORD.toString());
    } else {
      parseOptions.setOption(ParseOptions.OPTIONS_HEADER, ParseOptions.HEADER.NONE.toString());
    }

    if (null != csvParams) {
      parseOptions.setOption(ParseOptions.OPTIONS_CSV_DELIMITER, csvParams.getCsvDelimiter());
      parseOptions.setOption(ParseOptions.OPTIONS_CSV_ESCAPE_CHAR, csvParams.getCsvEscape());
      parseOptions.setOption(ParseOptions.OPTIONS_CSV_QUOTE, csvParams.getCsvQuote());
    }

    Reader reader = getInputStreamReader(uploadedInputStream);
    try (DataParser dataParser = new DataParser(reader, parseOptions)) {
      // encode column values into HEX so that \n etc dont appear in the hive table data
      Reader csvReader = new TableDataReader(dataParser.iterator(), header, containsEndlines);
      return uploadIntoTable(csvReader, databaseName, tableName, hiveContext);
    }
  }

  private String uploadIntoTable(Reader reader, String databaseName, String tempTableName, HiveContext hiveContext) {
    try {
      String fullPath = getHiveMetaStoreLocation(databaseName, tempTableName, hiveContext);
      log.info("Uploading file into : {}", fullPath);
      uploadFile(fullPath, new ReaderInputStream(reader), getHdfsApi(hiveContext));
      return fullPath;
    } catch (WebApplicationException e) {
      log.error(getErrorMessage(e), e);
      throw e;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new ServiceFormattedException(e);
    }
  }

  private HdfsApi getHdfsApi(HiveContext hiveContext) {
    if(!Boolean.valueOf(hiveConfiguration.get(Constants.HIVE_DO_AS_KEY, Constants.HIVE_DO_AS_VALUE))){
      return hdfsApiSupplier.get(new HdfsContext(authConfig.getAppUserName())).get();
    }else {
      return hdfsApiSupplier.get(hiveUtils.createHdfsContext(hiveContext)).get();
    }
  }

  private Reader getInputStreamReader(InputStream is) throws IOException {
    BOMInputStream bomInputStream = new BOMInputStream(is,
        ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE,
        ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE
    );
    if (bomInputStream.hasBOM()) {
      String charSetName = bomInputStream.getBOMCharsetName();
      return new InputStreamReader(bomInputStream, charSetName); // return with the encoded charset encoding.
    } else {
      return new InputStreamReader(bomInputStream); //return with default charset
    }
  }


}
