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
package com.hortonworks.hivestudio.eventProcessor.processors.stats;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Provider;

import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.eventProcessor.dto.ParsedPlan;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;

import lombok.Value;

public abstract class StatsProcessor {
  protected Provider<DatabaseRepository> databaseRepositoryProvider = null;
  protected Provider<TableRepository> tableRepositoryProvider = null;
  protected Provider<ColumnRepository> columnRepositoryProvider = null;

  public StatsProcessor(Provider<DatabaseRepository> databaseRepositoryProvider,
                           Provider<TableRepository> tableRepositoryProvider, Provider<ColumnRepository> columnRepositoryProvider) {
    this.databaseRepositoryProvider = databaseRepositoryProvider;
    this.tableRepositoryProvider = tableRepositoryProvider;
    this.columnRepositoryProvider = columnRepositoryProvider;
  }

  public abstract void updateCount(ParsedPlan plan);

  public abstract void updateCountsToDB();

  public abstract void rollupCounts(LocalDate date, SchedulerAuditType type);

  public Set<Table> getTablesFromDB(Map<String, Set<String>> dbToTables) {
    TableRepository tableRepository = tableRepositoryProvider.get();
    return new HashSet<>(tableRepository.getTableAndDatabaseByNames(dbToTables));
  }

  public Set<Column> getColumnsFromDB(Map<String, Map<String, Set<String>>> dbTableColumns) {
    ColumnRepository columnRepository = columnRepositoryProvider.get();
    return new HashSet<>(columnRepository.getAllByColumnAndTableAndDatabases(dbTableColumns));
  }

  @Value
  class TableDate {
    private final Integer tableId;
    private final LocalDate date;
  }

  @Value
  class ColumnDate {
    private final Integer columnId;
    private final LocalDate date;
  }

  @Value
  class JoinDate {
    private final Integer leftColumn;
    private final Integer rightColumn;
    private final String algorithm;
    private final LocalDate date;
  }

}
