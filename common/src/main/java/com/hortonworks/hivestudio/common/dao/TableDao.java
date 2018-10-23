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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

@RegisterBeanMapper(Table.class)
public interface TableDao extends JdbiDao<Table, Integer> {

  String NOT_DROPPED_CLAUSE = "AND (t.dropped is null or t.dropped = false)";

  @SqlQuery("SELECT * FROM das.tables WHERE id = :id")
  Optional<Table> findOne(@Bind("id") Integer id);

  @SqlQuery("SELECT * FROM das.tables")
  ArrayList<Table> findAll();

  @SqlQuery("SELECT * FROM das.tables WHERE id IN (<tableIds>)")
  List<Table> findAllByTableIds(@BindList("tableIds") List<Integer> tableIds);

  @SqlQuery("SELECT * FROM das.tables WHERE db_id = :databaseId")
  List<Table> findAllByDatabase(@Bind("databaseId") Integer databaseId);

  @SqlQuery("SELECT * FROM das.tables t WHERE t.db_id = :databaseId " + NOT_DROPPED_CLAUSE)
  List<Table> findAllForDatabaseAndNotDropped(@Bind("databaseId") Integer databaseId);

  @SqlQuery("SELECT * FROM das.tables t WHERE t.db_id = :databaseId AND t.creation_source = :creationSource " + NOT_DROPPED_CLAUSE)
  List<Table> findAllForDatabaseAndCreationSourceAndNotDropped(@Bind("databaseId") Integer databaseId, @Bind("creationSource") String creationSource);

  @SqlQuery("SELECT t.* FROM das.tables t JOIN das.databases d ON t.db_id = d.id WHERE d.name = :databaseName AND t.name in (<tableNames>)")
  List<Table> findAllForDatabaseNameAndTableNames(@Bind("databaseName") String databaseNames,
      @BindList("tableNames") Collection<String> tableNames);

  @SqlQuery("SELECT t.* FROM das.tables t JOIN das.databases d ON t.db_id = d.id WHERE d.name = :databaseName AND t.name = :tableName " + NOT_DROPPED_CLAUSE + " LIMIT 1")
  Optional<Table> findAllForDatabaseNameAndTableNameAndNotDropped(@Bind("databaseName") String databaseName, @Bind("tableName") String tableName);

  @SqlQuery("SELECT t.* FROM das.tables t JOIN das.databases d ON t.db_id = d.id WHERE d.name = :databaseName AND t.creation_source = :creationSource AND t.name = :tableName " + NOT_DROPPED_CLAUSE + " LIMIT 1")
  Optional<Table> findAllForDatabaseNameAndTableNameAndCreationSourceAndNotDropped(@Bind("databaseName") String databaseName, @Bind("tableName") String tableName, @Bind("creationSource") String creationSource);

  @SqlQuery("SELECT t.* FROM das.tables t WHERE t.db_id = :databaseId AND t.creation_source = :creationSource AND t.name = :tableName " + NOT_DROPPED_CLAUSE + " LIMIT 1")
  Optional<Table> findAllForDatabaseIdAndTableNameAndCreationSourceAndNotDropped(@Bind("databaseId") Integer databaseId, @Bind("tableName") String tableName, @Bind("creationSource") String creationSource);

  @SqlUpdate("INSERT into das.tables" +
    "(name, owner, create_time, last_access_time, parsed_table_type, table_type, location, serde," +
    "input_format, output_format, compressed, num_buckets, comment, dropped, dropped_at, last_updated_at," +
    "db_id, properties, creation_source, retention, storage_parameters)" +
    "VALUES" +
    "(:name, :owner, :createTime, :lastAccessTime, :parsedTableType, :tableType, :location, :serde, " +
    ":inputFormat, :outputFormat, :compressed, :numBuckets, :comment, :dropped, :droppedAt, :lastUpdatedAt, " +
    ":dbId, cast(:properties as jsonb), :creationSource, :retention, cast(:storageParameters as jsonb))")
  @GetGeneratedKeys
  Integer insert(@BindBean Table entity);

  @SqlUpdate("DELETE from das.tables where id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("UPDATE das.tables set " +
    "name = :name, owner = :owner, create_time = :createTime, last_access_time = :lastAccessTime," +
    "parsed_table_type = :parsedTableType, table_type = :tableType, location = :location, serde = :serde," +
    "input_format = :inputFormat, output_format = :outputFormat, compressed = :compressed, num_buckets = :numBuckets," +
    "comment = :comment, dropped = :dropped, dropped_at = :droppedAt, last_updated_at = :lastUpdatedAt," +
    "db_id = :dbId, properties = cast(:properties as jsonb), creation_source = :creationSource, retention = :retention, " +
    "storage_parameters = cast(:storageParameters as jsonb) where id = :id" )
  int update(@BindBean Table savedQuery);

  @SqlUpdate("INSERT INTO das.tables" +
    "(name, owner, create_time, last_access_time, parsed_table_type, table_type, location, serde," +
    "input_format, output_format, compressed, num_buckets, comment, dropped, dropped_at, last_updated_at," +
    "db_id, properties, creation_source, retention, storage_parameters)" +
    "VALUES " +
    "(:name, :owner, :createTime, :lastAccessTime, :parsedTableType, :tableType, :location, :serde, " +
    ":inputFormat, :outputFormat, :compressed, :numBuckets, :comment, :dropped, :droppedAt, :lastUpdatedAt, " +
    ":dbId, cast(:properties as jsonb), :creationSource, :retention, cast(:storageParameters as jsonb))" +
    "ON CONFLICT (name,db_id,dropped) WHERE not dropped DO UPDATE SET " +
    "owner = EXCLUDED.owner, create_time = EXCLUDED.create_time, last_access_time = EXCLUDED.last_access_time," +
    "parsed_table_type = EXCLUDED.parsed_table_type, table_type = EXCLUDED.table_type, location = EXCLUDED.location, serde = EXCLUDED.serde," +
    "input_format = EXCLUDED.input_format, output_format = EXCLUDED.output_format, compressed = EXCLUDED.compressed, num_buckets = EXCLUDED.num_buckets," +
    "comment = EXCLUDED.comment, dropped = EXCLUDED.dropped, dropped_at = EXCLUDED.dropped_at, last_updated_at = EXCLUDED.last_updated_at," +
    "db_id = EXCLUDED.db_id, properties = EXCLUDED.properties, creation_source = EXCLUDED.creation_source, retention = EXCLUDED.retention, " +
    "storage_parameters = cast(:storageParameters as jsonb)")
  @GetGeneratedKeys
  Table upsert(@BindBean Table table);
}
