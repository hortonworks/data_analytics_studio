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
package com.hortonworks.hivestudio.common.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;

import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class FileOperationService {

  private final HdfsApiSupplier hdfsApiSupplier;

  @Inject
  public FileOperationService(HdfsApiSupplier hdfsApiSupplier) {
    this.hdfsApiSupplier = hdfsApiSupplier;
  }

  public boolean deleteFile(String filePath, HdfsContext requestContext) throws IOException, InterruptedException {
    log.debug("Deleting file " + filePath);
    return getHdfsApi(requestContext).delete(filePath, false);
  }

  public void updateFile(FileResource request, String filePath, HdfsContext requestContext) throws IOException, InterruptedException {
    log.debug("Rewriting file " + filePath);
    try (FSDataOutputStream output = getHdfsApi(requestContext).create(filePath, true)) {
      output.writeBytes(request.getFileContent());
    }
  }

  public FileResource readFile(String filePath, Long page, HdfsContext requestContext) throws IOException, InterruptedException {
    FileResource file = new FileResource();
    HdfsApi hdfsApi = getHdfsApi(requestContext);
    FilePaginator paginator = new FilePaginator(filePath, hdfsApi);

    fillRealFileObject(filePath, page, file, paginator);

    return file;
  }

  public String readFileFully(String filePath, HdfsContext requestContext) throws IOException, InterruptedException {
    HdfsApi hdfsApi = getHdfsApi(requestContext);

    try(FSDataInputStream fileStream = hdfsApi.open(filePath);
        ByteArrayOutputStream baos = new ByteArrayOutputStream()){
      IOUtils.copy(fileStream, baos);
      return new String(baos.toByteArray());
    }
  }

  public InputStream getInputStream(String filePath, HdfsContext requestContext) throws IOException, InterruptedException {
    HdfsApi hdfsApi = getHdfsApi(requestContext);
    return hdfsApi.open(filePath);
  }

  public void fillRealFileObject(String filePath, Long page, FileResource file, FilePaginator paginator) throws IOException, InterruptedException {
    file.setFilePath(filePath);
    file.setFileContent(paginator.readPage(page));
    file.setHasNext(paginator.pageCount() > page + 1);
    file.setPage(page);
    file.setPageCount(paginator.pageCount());
  }

  public void createFile(FileResource request, HdfsContext requestContext) throws IOException, InterruptedException {
    log.debug("Creating file " + request.getFilePath());
    try (FSDataOutputStream output = getHdfsApi(requestContext).create(request.getFilePath(), false)) {
      if (request.getFileContent() != null) {
        output.writeBytes(request.getFileContent());
      }
    } catch (FileAlreadyExistsException ex) {
      throw new ServiceFormattedException("F020 File already exists", ex, 400);
    }
  }

  public HdfsApi getHdfsApi(HdfsContext requestContext) {
    return hdfsApiSupplier.get(requestContext).get();
  }
}
