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

import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;

import java.io.Serializable;
import java.util.List;

public class UploadFromHdfsInput implements Serializable{
  private Boolean isFirstRowHeader = Boolean.FALSE;
  private String inputFileType;
  private String hdfsPath;
  private String tableName;
  private String databaseName;
  private List<ColumnInfo> header;
  private boolean containsEndlines;

  private String csvDelimiter;
  private String csvEscape;
  private String csvQuote;

  public UploadFromHdfsInput() {
  }

  public String getCsvDelimiter() {
    return csvDelimiter;
  }

  public List<ColumnInfo> getHeader() {
    return header;
  }

  public void setHeader(List<ColumnInfo> header) {
    this.header = header;
  }

  public boolean isContainsEndlines() {
    return containsEndlines;
  }

  public void setContainsEndlines(boolean containsEndlines) {
    this.containsEndlines = containsEndlines;
  }

  public void setCsvDelimiter(String csvDelimiter) {
    this.csvDelimiter = csvDelimiter;
  }

  public String getCsvEscape() {
    return csvEscape;
  }

  public void setCsvEscape(String csvEscape) {
    this.csvEscape = csvEscape;
  }

  public String getCsvQuote() {
    return csvQuote;
  }

  public void setCsvQuote(String csvQuote) {
    this.csvQuote = csvQuote;
  }

  public Boolean getIsFirstRowHeader() {
    return isFirstRowHeader;
  }

  public void setIsFirstRowHeader(Boolean firstRowHeader) {
    isFirstRowHeader = firstRowHeader;
  }

  public String getInputFileType() {
    return inputFileType;
  }

  public void setInputFileType(String inputFileType) {
    this.inputFileType = inputFileType;
  }

  public String getHdfsPath() {
    return hdfsPath;
  }

  public void setHdfsPath(String hdfsPath) {
    this.hdfsPath = hdfsPath;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public String toString() {
    return new StringBuilder("UploadFromHdfsInput{" )
            .append("isFirstRowHeader=").append( isFirstRowHeader )
            .append(", inputFileType='" ).append(inputFileType)
            .append(", hdfsPath='").append(hdfsPath)
            .append(", tableName='").append( tableName )
            .append(", databaseName='").append(databaseName )
            .append('}').toString();
  }
}
