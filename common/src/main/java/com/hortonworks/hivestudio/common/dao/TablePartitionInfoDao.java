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

import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.common.repository.JdbiDao;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

@RegisterBeanMapper(TablePartitionInfo.class)
public interface TablePartitionInfoDao extends JdbiDao<TablePartitionInfo, Integer> {

  @SqlQuery("SELECT * FROM das.table_partition_info WHERE id = :id")
  Optional<TablePartitionInfo> findOne(@Bind("id") Integer id);

  @SqlQuery("SELECT * FROM das.table_partition_info WHERE table_id = :tableId AND partition_name = :partitionName")
  Optional<TablePartitionInfo> findOneByTableIdAndPartitionName(@Bind("tableId") Integer tableId, @Bind("partitionName") String partitionName);

  @SqlQuery("SELECT * FROM das.table_partition_info")
  List<TablePartitionInfo> findAll();

  @SqlQuery("SELECT tpi.* FROM das.table_partition_info tpi JOIN das.tables t ON tpi.table_id = t.id WHERE t.id = :tableId " + TableDao.NOT_DROPPED_CLAUSE)
  List<TablePartitionInfo> findAllForTableAndNorDropped(@Bind("tableId") Integer tableId);

  @SqlUpdate("INSERT INTO das.table_partition_info (table_id, partition_name, details, raw_data_size, num_rows, num_files)" +
    "VALUES (:tableId, :partitionName, cast(:details as jsonb), :rawDataSize, :numRows, :numFiles)")
  @GetGeneratedKeys
  Integer insert(@BindBean TablePartitionInfo entity);

  @SqlUpdate("DELETE FROM das.table_partition_info WHERE id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("UPDATE das.table_partition_info SET " +
    "table_id = :tableId, partition_name = :partitionName, details = :details, raw_data_size = :rawDataSize," +
    "num_rows = :numRows, num_files = :numFiles WHERE id = :id" )
  int update(@BindBean TablePartitionInfo savedQuery);

  @SqlUpdate("INSERT INTO das.table_partition_info" +
    "(table_id, partition_name, details, raw_data_size, num_rows, num_files) " +
    "VALUES " +
    "(:tableId, :partitionName, cast(:details as jsonb), :rawDataSize, :numRows, :numFiles) " +
    "ON CONFLICT (table_id,partition_name) DO UPDATE SET " +
    "table_id = EXCLUDED.table_id, partition_name = EXCLUDED.partition_name, details = EXCLUDED.details, raw_data_size = EXCLUDED.raw_data_size, " +
    "num_rows = EXCLUDED.num_rows, num_files = EXCLUDED.num_files")
  int upsert(@BindBean TablePartitionInfo tablePartitionInfo);

}