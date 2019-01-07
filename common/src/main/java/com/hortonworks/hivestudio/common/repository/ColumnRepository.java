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
package com.hortonworks.hivestudio.common.repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Collections2;
import com.google.inject.Inject;
import com.hortonworks.hivestudio.common.dao.ColumnDao;
import com.hortonworks.hivestudio.common.entities.Column;


public class ColumnRepository extends JdbiRepository<Column, Integer, ColumnDao> {

  @Inject
  public ColumnRepository(ColumnDao dao) {
    super(dao);
  }

  @Override
  public Optional<Column> findOne(Integer id) {
    return dao.findOne(id);
  }

  @Override
  public Collection<Column> findAll() {
    return dao.findAll();
  }

  @Override
  public Column save(Column entity) {
    if (entity.getId() == null) {
      Integer id = dao.insert(entity);
      entity.setId(id);
    } else {
      dao.update(entity);
    }

    return entity;
  }

  @Override
  public boolean delete(Integer id) {
    return !(0 == dao.delete(id));
  }

  public Column upsert(Column entity) {
    return dao.upsert(entity);
  }

  public List<Column> getAllForTableNotDropped(Integer tableId) {
    return dao.findAllByTableIdAndNotDropped(tableId);
  }

  public Map<Integer, List<Column>> getAllForTablesGroupedByTable(List<Integer> tableIds) {
    List<Column> columns = dao.findAllByTableIds(tableIds);
    return groupByTable(columns);
  }

  public Map<Integer, List<Column>> getAllForDatabaseGroupedByTable(Integer databaseId) {
    List<Column> columns = dao.findAllByDatabase(databaseId);
    return groupByTable(columns);
  }

  public List<Column> getAllByColumnAndTableAndDatabases(
      Map<String, Map<String, Set<String>>> dbTableColumns) {
    List<Column> results = new ArrayList<>();
    for (Entry<String, Map<String, Set<String>>> dbEntry : dbTableColumns.entrySet()) {
      String dbName = dbEntry.getKey().toLowerCase();
      for (Entry<String, Set<String>> tblEntry : dbEntry.getValue().entrySet()) {
        String tableName = tblEntry.getKey().toLowerCase();
        Collection<String> columns = Collections2.transform(tblEntry.getValue(), String::toLowerCase);
        results.addAll(dao.findAllByColumnAndTableAndDatabase(columns, tableName, dbName));
      }
    }
    return results;
  }

  public int markColumnDroppedForTable(Integer tableId, Date droppedAt) {
    return dao.markDroppedForTable(tableId, droppedAt);
  }

  private Map<Integer, List<Column>> groupByTable(List<Column> columns) {
    return columns
        .stream()
        .collect(Collectors.groupingBy(column -> column.getTableId(), Collectors.toList()));
  }

}
