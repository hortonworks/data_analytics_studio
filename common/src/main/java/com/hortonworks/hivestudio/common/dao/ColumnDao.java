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
package com.hortonworks.hivestudio.common.dao;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

@RegisterBeanMapper(Column.class)
public interface ColumnDao extends JdbiDao<Column, Integer> {

  @SqlQuery("SELECT * FROM das.columns WHERE id = :id")
  Optional<Column> findOne(@Bind("id") Integer id);

  @SqlQuery("SELECT * FROM das.columns")
  List<Column> findAll();

  @SqlQuery("SELECT * FROM das.columns WHERE table_id = :tableId and dropped = false ORDER BY column_position")
  List<Column> findAllByTableIdAndNotDropped(@Bind("tableId") Integer tableId);

  @SqlQuery("SELECT * FROM das.columns WHERE table_id IN (<tableIds>) ORDER BY table_id, column_position")
  List<Column> findAllByTableIds(@BindList("tableIds") List<Integer> tableIds);

  @SqlQuery("SELECT c.* FROM das.columns c JOIN das.tables t on c.table_id = t.id WHERE t.db_id = :databaseId  ORDER BY column_position")
  List<Column> findAllByDatabase(@Bind("databaseId") Integer databaseId);

  @SqlQuery("SELECT c.* FROM das.columns c JOIN das.tables t ON c.table_id = t.id " +
      "JOIN das.databases d ON t.db_id = d.id WHERE c.name IN (<columnNames>) " +
      "AND t.name = :tableName AND d.name = :databaseName")
  List<Column> findAllByColumnAndTableAndDatabase(
      @BindList("columnNames") Collection<String> columnNames,
      @Bind("tableName") String tableName,
      @Bind("databaseName") String databaseName);

  @SqlUpdate("INSERT INTO das.columns" +
  "(name, datatype, column_type, precision, scale, comment, create_time, is_primary," +
    "is_partitioned, is_clustered, is_sort_key, sort_order, dropped, dropped_at, table_id," +
    "creation_source, column_position) VALUES" +
  "(:name, :datatype, :columnType, :precision, :scale, :comment, :createTime, :isPrimary," +
    ":isPartitioned, :isClustered, :isSortKey, :sortOrder, :dropped, :droppedAt, :tableId," +
    ":creationSource, :columnPosition)")
  @GetGeneratedKeys
  Integer insert(@BindBean Column column);

  @SqlUpdate("DELETE FROM das.columns WHERE id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("UPDATE das.columns SET" +
  "name = :name, datatype = :datatype, column_type = :columnType, precision = :precision," +
  "scale = :scale, comment = :comment, create_time = :createTime, is_primary = :isPrimary," +
  "is_partitioned = :isPartitioned, is_clustered = :isClustered, is_sort_key = :isSortKey," +
  "sort_order = :sortOrder, dropped = :dropped, dropped_at = :droppedAt, table_id = :tableId," +
  "creation_source = :creationSource, column_position = :columnPosition WHERE id = :id" )
  int update(@BindBean Column column);

  @SqlUpdate("INSERT INTO das.columns" +
    "(name, datatype, column_type, precision, scale, comment, create_time, is_primary," +
    "is_partitioned, is_clustered, is_sort_key, sort_order, dropped, dropped_at, table_id," +
    "creation_source, column_position) values" +
    "(:name, :datatype, :columnType, :precision, :scale, :comment, :createTime, :isPrimary," +
    ":isPartitioned, :isClustered, :isSortKey, :sortOrder, :dropped, :droppedAt, :tableId," +
    ":creationSource, :columnPosition)" +
    "ON CONFLICT (name,table_id,dropped) WHERE not dropped DO UPDATE SET " +
    "datatype = EXCLUDED.datatype, column_type = EXCLUDED.column_type, precision = EXCLUDED.precision," +
    "scale = EXCLUDED.scale, comment = EXCLUDED.comment, create_time = EXCLUDED.create_time, is_primary = EXCLUDED.is_primary," +
    "is_partitioned = EXCLUDED.is_partitioned, is_clustered = EXCLUDED.is_clustered, is_sort_key = EXCLUDED.is_sort_key," +
    "sort_order = EXCLUDED.sort_order, dropped_at = EXCLUDED.dropped_at, creation_source = EXCLUDED.creation_source, column_position = EXCLUDED.column_position")
  @GetGeneratedKeys
  Column upsert(@BindBean Column column);

  @SqlUpdate("UPDATE das.columns SET dropped = true, dropped_at = :droppedAt WHERE table_id = :tableId AND dropped = false")
  int markDroppedForTable(@Bind("tableId") Integer tableId, @Bind("droppedAt") Date droppedAt);
}
