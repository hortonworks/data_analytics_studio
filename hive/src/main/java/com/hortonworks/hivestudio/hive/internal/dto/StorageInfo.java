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


package com.hortonworks.hivestudio.hive.internal.dto;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class StorageInfo {
  private String serdeLibrary;
  private String inputFormat;
  private String outputFormat;
  private String compressed;
  private String numBuckets;
  private List<String> bucketCols;
  private List<ColumnOrder> sortCols;
  private String fileFormat;
  private Map<String, String> parameters;

  public String getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(String fileFormat) {
    this.fileFormat = fileFormat;
  }

  public String getSerdeLibrary() {
    return serdeLibrary;
  }

  public void setSerdeLibrary(String serdeLibrary) {
    this.serdeLibrary = serdeLibrary;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public String getCompressed() {
    return compressed;
  }

  public void setCompressed(String compressed) {
    this.compressed = compressed;
  }

  public String getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(String numBuckets) {
    this.numBuckets = numBuckets;
  }

  public List<String> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  public List<ColumnOrder> getSortCols() {
    return sortCols;
  }

  public void setSortCols(List<ColumnOrder> sortCols) {
    this.sortCols = sortCols;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String toString() {
    return "StorageInfo{" +
        "serdeLibrary='" + serdeLibrary + '\'' +
        ", inputFormat='" + inputFormat + '\'' +
        ", outputFormat='" + outputFormat + '\'' +
        ", compressed='" + compressed + '\'' +
        ", numBuckets='" + numBuckets + '\'' +
        ", bucketCols='" + bucketCols + '\'' +
        ", sortCols='" + sortCols + '\'' +
        ", fileFormat='" + fileFormat + '\'' +
        ", parameters=" + parameters +
        '}';
  }
}
