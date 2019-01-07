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

import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.AuthConfig;
import com.hortonworks.hivestudio.common.config.HiveConfiguration;
import com.hortonworks.hivestudio.common.hdfs.HdfsApi;
import com.hortonworks.hivestudio.common.hdfs.HdfsApiSupplier;
import com.hortonworks.hivestudio.common.hdfs.HdfsContext;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.HiveUtils;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.services.uploads.parsers.PreviewData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UploadServiceTest {

  private UploadService uploadService;
  private HiveConfiguration hiveConfiguration;
  private AuthConfig authConfig;
  private HdfsApiSupplier hdfsApiSupplier;
  private HiveUtils hiveUtils;

  @Before
  public void setup(){
    hiveConfiguration = mock(HiveConfiguration.class);
    authConfig = mock(AuthConfig.class);
    hdfsApiSupplier = mock(HdfsApiSupplier.class);
    hiveUtils = mock(HiveUtils.class);
    uploadService = new UploadService(null, null, hdfsApiSupplier, hiveUtils, null, hiveConfiguration, authConfig) {
      //      TODO : Bad design. it should not be required to override getHiveMetaStoreLocation() for testing. Update getHiveMetaStoreLocation() to better handle tests.
      @Override
      public String getHiveMetaStoreLocation(String db, String table, HiveContext hiveContext) {
        return "/some/hardcoded/path";
      }

      @Override
      void uploadFile(final String filePath, InputStream inputStream, HdfsApi hdfsApi) {
//          does nothing
      }
    };

  }
  @Test
  public void generatePreviewWithBOM() throws Exception {
    // convert String into InputStream
    String str = "\ufeffCol1\tCol2\nA\tB\n";
    InputStream inputStream = new ByteArrayInputStream(str.getBytes());
    PreviewData previewData = uploadService.generatePreview(true, "CSV", new CSVParams('\t', '\"', '\\'), inputStream);

    Assert.assertEquals("Incorrect number of columns detected.", 2, previewData.getHeader().size() );
    Assert.assertEquals("incorrect col objects.", Arrays.asList(new ColumnInfo("Col1", "CHAR", null, null, null),
        new ColumnInfo("Col2", "CHAR", null, null, null)), previewData.getHeader());
    Assert.assertEquals("incorrect row objects.", Arrays.asList(new Row(new Object[]{"A", "B"})), previewData.getPreviewRows());
  }

  @Test
  public void generatePreviewWithoutBOM() throws Exception {
    UploadService uploadService = new UploadService(null, null, null, null, null, null, null);
    // convert String into InputStream
    String str = "Col1\tCol2\nA\tB\n";
    InputStream inputStream = new ByteArrayInputStream(str.getBytes());
    PreviewData previewData = uploadService.generatePreview(true, "CSV", new CSVParams('\t', '\"', '\\'), inputStream);

    Assert.assertEquals("Incorrect number of columns detected.", 2, previewData.getHeader().size() );
    Assert.assertEquals("incorrect col objects.", Arrays.asList(new ColumnInfo("Col1", "CHAR", null, null, null),
        new ColumnInfo("Col2", "CHAR", null, null, null)), previewData.getHeader());
    Assert.assertEquals("incorrect row objects.", Arrays.asList(new Row(new Object[]{"A", "B"})), previewData.getPreviewRows());
  }

  @Test
  public void with_DoAs_True() throws Exception {
    String str = "Col1\tCol2\nA\tB\n";
    InputStream inputStream = new ByteArrayInputStream(str.getBytes());
    List<ColumnInfo> header = new ArrayList<>();
    header.add(new ColumnInfo("col1", "string"));
    header.add(new ColumnInfo("col2", "string"));
    String signInUser = "signInUser";
    HiveContext hiveContext = new HiveContext(signInUser);
    when(authConfig.getAppUserName()).thenReturn("hive");

    HdfsApi hdfsApi = mock(HdfsApi.class);

    HdfsContext hdfsContext = new HdfsContext(hiveContext.getUsername());
    when(hiveUtils.createHdfsContext(hiveContext)).thenReturn(hdfsContext);
    when(hdfsApiSupplier.get(hdfsContext)).thenReturn(Optional.of(hdfsApi));
    when(hiveConfiguration.get(Constants.HIVE_DO_AS_KEY, Constants.HIVE_DO_AS_VALUE)).thenReturn("True");

    String uploadedFilePath = uploadService.uploadFileFromStream(inputStream, true, "CSV",
        "sometable", "somedatabase", header, false,
        new CSVParams('\t', '\"', '\\'),hiveContext );

    ArgumentCaptor<HdfsContext> argument = ArgumentCaptor.forClass(HdfsContext.class);
    verify(hdfsApiSupplier, times(1)).get(argument.capture());
    verify(authConfig, times(0)).getAppUserName();

    HdfsContext value = argument.getValue();
    Assert.assertEquals(signInUser, value.getUsername());
  }

  @Test
  public void with_DoAs_False() throws Exception {
    String str = "Col1\tCol2\nA\tB\n";
    InputStream inputStream = new ByteArrayInputStream(str.getBytes());
    List<ColumnInfo> header = new ArrayList<>();
    header.add(new ColumnInfo("col1", "string"));
    header.add(new ColumnInfo("col2", "string"));
    String signInUser = "signInUser";
    String appUser = "appUser";
    HiveContext hiveContext = new HiveContext(signInUser);
    when(authConfig.getAppUserName()).thenReturn(appUser);

    HdfsApi hdfsApi = mock(HdfsApi.class);

    HdfsContext hdfsContext = new HdfsContext(hiveContext.getUsername());
    when(hiveUtils.createHdfsContext(hiveContext)).thenReturn(hdfsContext);
    when(hdfsApiSupplier.get(any())).thenReturn(Optional.of(hdfsApi));

    when(hiveConfiguration.get(Constants.HIVE_DO_AS_KEY, Constants.HIVE_DO_AS_VALUE)).thenReturn("False");

    String uploadedFilePath = uploadService.uploadFileFromStream(inputStream, true, "CSV",
        "sometable", "somedatabase", header, false,
        new CSVParams('\t', '\"', '\\'),hiveContext );

    ArgumentCaptor<HdfsContext> argument = ArgumentCaptor.forClass(HdfsContext.class);
    verify(hdfsApiSupplier, times(1)).get(argument.capture());
    verify(authConfig, times(1)).getAppUserName();

    HdfsContext value = argument.getValue();
    Assert.assertEquals(appUser, value.getUsername());
  }
}