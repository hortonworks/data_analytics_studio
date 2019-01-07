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


import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.hortonworks.hivestudio.common.config.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HdfsUtil {
  /**
   * Write string to file with overwriting
   * @param filePath path to file
   * @param content new content of file
   */
  public static void putStringToFile(HdfsApi hdfs, String filePath, String content) throws HdfsApiException {
    FSDataOutputStream stream;
    try {
      synchronized (hdfs) {
        stream = hdfs.create(filePath, true);
        stream.write(content.getBytes());
        stream.close();
      }
    } catch (IOException e) {
      throw new HdfsApiException("HDFS020 Could not write file " + filePath, e);
    } catch (InterruptedException e) {
      throw new HdfsApiException("HDFS021 Could not write file " + filePath, e);
    }
  }

  /**
   * Read string from file
   * @param filePath path to file
   */
  public static String readFile(HdfsApi hdfs, String filePath) throws HdfsApiException {
    FSDataInputStream stream;
    try {
      stream = hdfs.open(filePath);
      return IOUtils.toString(stream);
    } catch (IOException e) {
      throw new HdfsApiException("HDFS060 Could not read file " + filePath, e);
    } catch (InterruptedException e) {
      throw new HdfsApiException("HDFS061 Could not read file " + filePath, e);
    }
  }


  /**
   * Increment index appended to filename until find first unallocated file
   * @param fullPathAndFilename path to file and prefix for filename
   * @param extension file extension
   * @return if fullPathAndFilename="/tmp/file",extension=".txt" then filename will be like "/tmp/file_42.txt"
   */
  public static String findUnallocatedFileName(HdfsApi hdfs, String fullPathAndFilename, String extension)
      throws HdfsApiException {
    int triesCount = 0;
    String newFilePath;
    boolean isUnallocatedFilenameFound;

    try {
      do {
        newFilePath = String.format(fullPathAndFilename + "%s" + extension, (triesCount == 0) ? "" : "_" + triesCount);
        log.debug("Trying to find free filename " + newFilePath);

        isUnallocatedFilenameFound = !hdfs.exists(newFilePath);
        if (isUnallocatedFilenameFound) {
          log.debug("File created successfully!");
        }

        triesCount += 1;
        if (triesCount > 1000) {
          throw new HdfsApiException("HDFS100 Can't find unallocated file name " + fullPathAndFilename + "...");
        }
      } while (!isUnallocatedFilenameFound);
    } catch (IOException e) {
      throw new HdfsApiException("HDFS030 Error in creation " + fullPathAndFilename + "...", e);
    } catch (InterruptedException e) {
      throw new HdfsApiException("HDFS031 Error in creation " + fullPathAndFilename + "...", e);
    }

    return newFilePath;
  }

  /**
   * takes any custom properties that a view wants to be included into the config
   * @param context
   * @param hadoopConfiguration
   *@param customViewProperties  @return
   * @throws HdfsApiException
   */
  public static synchronized HdfsApi connectToHDFSApi(HdfsContext context,
      org.apache.hadoop.conf.Configuration hadoopConfiguration,
      Map<String, String> customViewProperties) throws HdfsApiException {
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(HdfsUtil.class.getClassLoader());
    try {
      return getHdfsApi(context, hadoopConfiguration);
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  /**
   * Factory of HdfsApi for specific HdfsContext
   * @param context HdfsContext that contains connection credentials
   * @param hadoopConfiguration
   * @return HdfsApi object
   */
  public static synchronized HdfsApi connectToHDFSApi(HdfsContext context,
      org.apache.hadoop.conf.Configuration hadoopConfiguration) throws HdfsApiException {
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(HdfsUtil.class.getClassLoader());
    try {
      return getHdfsApi(context, hadoopConfiguration);
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  private static HdfsApi getHdfsApi(HdfsContext context, org.apache.hadoop.conf.Configuration hadoopConf) throws HdfsApiException {
    HdfsApi api = null;
    try {
      api = new HdfsApi(hadoopConf, context.getUsername());
      log.info("HdfsApi connected OK");
    } catch (IOException e) {
      log.error("exception occurred while creating hdfsApi objcet : {}", e.getMessage(), e);
      String message = "HDFS040 Couldn't open connection to HDFS";
      log.error(message);
      throw new HdfsApiException(message, e);
    } catch (InterruptedException e) {
      log.error("exception occurred while creating hdfsApi objcet : {}", e.getMessage(), e);
      String message = "HDFS041 Couldn't open connection to HDFS";
      log.error(message);
      throw new HdfsApiException(message, e);
    }
    return api;
  }

  /**
   * Returns username for HdfsApi from "webhdfs.username" property if set,
   * if not set then current Ambari username
   * @param context HdfsContext
   * @return username
   */
  public static String getHdfsUsername(HdfsContext context, Configuration configuration) {
    Optional<String> userName = configuration.get("webhdfs.username");
    return userName.orElse(context.getUsername());
  }
}
