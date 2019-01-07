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
package com.hortonworks.hivestudio.hive.internal.generators;

import com.google.common.base.Strings;
import com.hortonworks.hivestudio.hive.services.HiveServiceException;

import java.util.Optional;

public class RenameTableQueryGenerator implements QueryGenerator {
  private String oldDatabaseName;
  private final String oldTableName;
  private final String newDatabaseName;
  private final String newTableName;

  public RenameTableQueryGenerator(String oldDatabaseName, String oldTableName, String newDatabaseName, String newTableName) {
    this.oldDatabaseName = oldDatabaseName;
    this.oldTableName = oldTableName;
    this.newDatabaseName = newDatabaseName;
    this.newTableName = newTableName;
  }

  public String getOldDatabaseName() {
    return oldDatabaseName;
  }

  public String getOldTableName() {
    return oldTableName;
  }

  public String getNewDatabaseName() {
    return newDatabaseName;
  }

  public String getNewTableName() {
    return newTableName;
  }

  /**
   * ALTER TABLE table_name RENAME TO new_table_name;
   * @return Optional rename query if table has changed.
   */
  @Override
  public java.util.Optional<String> getQuery() {
    StringBuilder queryBuilder = new StringBuilder("ALTER TABLE `");
    if(!Strings.isNullOrEmpty(this.getOldDatabaseName())){
      queryBuilder.append(this.getOldDatabaseName().trim()).append("`.`");
    }
    if(!Strings.isNullOrEmpty(this.getOldTableName())){
      queryBuilder.append(this.getOldTableName().trim());
    }else{
      throw new HiveServiceException("current table name cannot be null or empty.");
    }
    queryBuilder.append("` RENAME TO `");

    if(!Strings.isNullOrEmpty(this.getNewDatabaseName())){
      queryBuilder.append(this.getNewDatabaseName().trim()).append("`.`");
    }

    if(!Strings.isNullOrEmpty(this.getNewTableName())){
      queryBuilder.append(this.getNewTableName().trim());
    }else{
      throw new HiveServiceException("new table name cannot be null or empty.");
    }

    queryBuilder.append("`");
    return Optional.of(queryBuilder.toString());
  }
}
