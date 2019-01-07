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
package com.hortonworks.hivestudio.hive.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class MetaStoreServiceTest {

  @Mock Provider<DatabaseRepository> databaseRepositoryProvider;
  @Mock Provider<TableRepository> tableRepositoryProvider;
  @Mock Provider<ColumnRepository> columnRepositoryProvider;
  @Mock ObjectMapper objectMapper;

  @Mock DatabaseRepository databaseRepository;
  @Mock TableRepository tableRepository;
  @Mock ColumnRepository columnRepository;

  MetaStoreService metaStoreService;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(databaseRepositoryProvider.get()).thenReturn(databaseRepository);
    when(tableRepositoryProvider.get()).thenReturn(tableRepository);
    when(columnRepositoryProvider.get()).thenReturn(columnRepository);

    metaStoreService = new MetaStoreService(databaseRepositoryProvider, tableRepositoryProvider,
      columnRepositoryProvider, objectMapper);
  }

  @After
  public void tearDown() throws Exception {
  }

  private Column createColumn() {
    Column column = new Column();
    column.setDropped(false);
    column.setIsPartitioned(false);
    column.setIsClustered(false);
    column.setSortOrder(0);
    column.setCreationSource(CreationSource.REPLICATION);
    return column;
  }

  @Test
  public void createTableMeta() throws Exception {

    Database database = new Database();
    database.setName("db_1");
    when(databaseRepository.findOne(any())).thenReturn(Optional.of(new Database()));

    Table table = new Table();
    table.setName("tb_1");
    table.setParsedTableType(ParsedTableType.NORMAL);

    List<Column> columns = Arrays.asList(createColumn(), createColumn(), createColumn());
    when(columnRepository.getAllForTableNotDropped(any())).thenReturn(columns);

    TableMeta meta = metaStoreService.createTableMeta(table);
    Assert.assertNotNull(meta);
    Assert.assertEquals("Columns not filtered based on creation source",3, meta.getColumns().size());

    columns.get(1).setCreationSource(CreationSource.EVENT_PROCESSOR);
    meta = metaStoreService.createTableMeta(table);
    Assert.assertNotNull(meta);
    Assert.assertEquals("Columns not filtered based on creation source",2, meta.getColumns().size());
  }

}