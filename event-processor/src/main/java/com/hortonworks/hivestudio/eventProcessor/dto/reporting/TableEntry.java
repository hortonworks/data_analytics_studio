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
package com.hortonworks.hivestudio.eventProcessor.dto.reporting;

import java.time.LocalDate;

import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.common.entities.Table;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(exclude = {"tableType"})
public final class TableEntry {
  private final String databaseName;
  private final String tableName;
  private final LocalDate date;
  private final ParsedTableType tableType;

  /**
   * converts databaseName and tablename to lower case and create TableEntry
   * @param databaseName
   * @param tableName
   * @param date
   * @param tableType
   * @throws NullPointerException if databaseName or tableName is null.
   */
  public TableEntry(String databaseName, String tableName, LocalDate date, ParsedTableType tableType) {
    this.databaseName = databaseName.toLowerCase();
    this.tableName = tableName.toLowerCase();
    this.date = date;
    this.tableType = tableType;
  }

  /**
   * converts databaseName and tablename to lower case and creates a TableEntry with {@link #tableType} set to
    {@link ParsedTableType#NORMAL}
   * @param databaseName
   * @param tableName
   * @param date
   * @throws NullPointerException if databaseName or tableName is null.
   */
  public TableEntry(String databaseName, String tableName, LocalDate date) {
    this(databaseName, tableName, date, ParsedTableType.NORMAL);
  }

  public TableEntry(String databaseName, String tableName) {
    this(databaseName, tableName, LocalDate.now());
  }

  public TableEntry(String databaseName, String tableName, ParsedTableType tableType) {
    this(databaseName, tableName, LocalDate.now(), tableType);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public LocalDate getDate() {
    return date;
  }

  public TableEntry getForToday() {
//    TODO : This introduces a bug as all the old queries in hive events will increase
//    count for today.
    return new TableEntry(this.databaseName, this.tableName, LocalDate.now(), tableType);
  }

//  public static TableEntry from(Table table, LocalDate date) {
//    return new TableEntry(table.getDatabase().getName(), table.getName(), date);
//  }

  public static TableEntry from(Table table, Database database) {
    return new TableEntry(database.getName(), table.getName());
  }

  public static TableEntry from(com.hortonworks.hivestudio.hivetools.parsers.entities.Table table) {
    return new TableEntry(table.getDatabaseName(), table.getName());
  }

  public static TableEntry from(com.hortonworks.hivestudio.hivetools.parsers.entities.Table table,
      LocalDate date) {
    return new TableEntry(table.getDatabaseName(), table.getName(), date);
  }

}
