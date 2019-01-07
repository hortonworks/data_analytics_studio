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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.collect.Collections2;
import com.hortonworks.hivestudio.common.dao.TableDao;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.common.entities.Table;

public class TableRepository extends JdbiRepository<Table, Integer, TableDao> {

  @Inject
  public TableRepository(TableDao dao) {
    super(dao);
  }

  @Override
  public Optional<Table> findOne(Integer id) {
    return dao.findOne(id);
  }

  @Override
  public Collection<Table> findAll() {
    return dao.findAll();
  }

  @Override
  public Table save(Table entity) {
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

  public Table upsert(Table entity) {
    return dao.upsert(entity);
  }

  public List<Table> getAllForDatabase(Integer databaseId) {
    return dao.findAllByDatabase(databaseId);
  }

  public List<Table> getAllForDatabaseAndNotDropped(Integer databaseId) {
    return dao.findAllForDatabaseAndNotDropped(databaseId);
  }

  public List<Table> getAllForDatabaseAndNotDroppedAndSynced(Integer databaseId) {
    return dao.findAllForDatabaseAndCreationSourceAndNotDropped(databaseId, CreationSource.REPLICATION.name());
  }

  public List<Table> getAllForTables(List<Integer> tableIds) {
    return dao.findAllByTableIds(tableIds);
  }

  // Map of database to table names.
  public List<Table> getTableAndDatabaseByNames(Map<String, Set<String>> dbToTables) {
    List<Table> results = new ArrayList<>();
    for (Entry<String, Set<String>> entry : dbToTables.entrySet()) {
      String dbName = entry.getKey().toLowerCase();
      Collection<String> tableNames = Collections2.transform(entry.getValue(), String::toLowerCase);
      results.addAll(dao.findAllForDatabaseNameAndTableNames(dbName, tableNames));
    }
    return results;
  }

  public Table getByDBNameTableNameAndNotDropped(String databaseName, String tableName) {
    return dao.findAllForDatabaseNameAndTableNameAndNotDropped(
        databaseName.toLowerCase(), tableName.toLowerCase()).orElse(null);
  }

  public Table getByDBNameTableNameAndNotDroppedAndSynced(String databaseName, String tableName) {
    return dao.findAllForDatabaseNameAndTableNameAndCreationSourceAndNotDropped(
        databaseName.toLowerCase(), tableName.toLowerCase(), CreationSource.REPLICATION.name())
      .orElse(null);
  }

  public Table getByDatabaseIdAndTableNameAndNotDroppedAndSynced(Integer databaseId, String tableName) {
    return dao.findAllForDatabaseIdAndTableNameAndCreationSourceAndNotDropped(databaseId,
        tableName.toLowerCase(), CreationSource.REPLICATION.name()).orElse(null);
  }
}