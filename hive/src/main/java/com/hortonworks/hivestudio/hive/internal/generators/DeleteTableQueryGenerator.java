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

public class DeleteTableQueryGenerator implements QueryGenerator{
  private final String databaseName;
  private final String tableName;
  private Boolean purge = Boolean.FALSE;

  public DeleteTableQueryGenerator(String databaseName, String tableName) {
    this(databaseName, tableName, Boolean.FALSE);
  }

  public DeleteTableQueryGenerator(String databaseName, String tableName, Boolean purge) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    if( null != purge ) this.purge = purge;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public Boolean getPurge() {
    return purge;
  }

  public void setPurge(Boolean purge) {
    this.purge = purge;
  }

  /**
   * @return
   */
  @Override
  public Optional<String> getQuery() {
    if(Strings.isNullOrEmpty(this.getDatabaseName()) || Strings.isNullOrEmpty(this.getTableName()))
      throw new HiveServiceException("databaseName or tableName was null.");

    return Optional.of("DROP TABLE `" + databaseName + "`.`" + tableName + "`" + (this.getPurge() ? " PURGE " : ""));
  }
}
