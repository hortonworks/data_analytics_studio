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
package com.hortonworks.hivestudio.hivetools.recommendations.analyzers.table;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.ColumnInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.util.ArrayUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TableMetaAnalyzer implements TableAnalyzer {

  @VisibleForTesting
  static final String NO_ORC_MSG = "Table %1$s is in plain text format. Hive works best with ORC file format. Create a new table using CREATE TABLE new_table ( columns ) STORED AS ORC; and run INSERT OVERWRITE TABLE new_table SELECT * from %1$s to convert existing table to ORC file format. Note this creates a copy of your table, for large tables this will be an expensive operation. Consult your database administrator before doing conversion on large tables to avoid running out of space.";
  @VisibleForTesting
  static final String STATS_WITH_PARTITION_MSG =    "Table %1$s doesn’t have statistics. This means Hive can’t optimize your query using cost based optimizer and your query execution is likely slow. Run ANALYZE TABLE %1$s PARTITION(%2$s) COMPUTE STATISTICS; ANALYZE TABLE %1$s PARTITION(%2$s) COMPUTE STATISTICS for COLUMNS; to gather statistics.";
  @VisibleForTesting
  static final String STATS_WITHOUT_PARTITION_MSG = "Table %1$s doesn’t have statistics. This means Hive can’t optimize your query using cost based optimizer and your query execution is likely slow. Run ANALYZE TABLE %1$s COMPUTE STATISTICS; ANALYZE TABLE %1$s COMPUTE STATISTICS for COLUMNS; to gather statistics.";

  public ArrayList<Recommendation> analyze(TableMeta table, Collection<TablePartitionInfo> partitions) {
    ArrayList<Recommendation> recommendations = new ArrayList<>();

    if(!table.getStorageInfo().getSerdeLibrary().endsWith("OrcSerde")) {
      recommendations.add(new Recommendation(String.format(NO_ORC_MSG, table.getTable())));
    }

    if(!table.getTableStats().getTableStatsEnabled()) {
      String tableName = table.getTable();
      List<ColumnInfo> partitionedColumns = table.getPartitionInfo().getColumns();
      if(partitionedColumns.size() == 0) {
        recommendations.add(new Recommendation(String.format(STATS_WITHOUT_PARTITION_MSG, tableName)));
      }
      else {
        ArrayList<String> columnNames = new ArrayList();
        for (ColumnInfo column : partitionedColumns) {
          columnNames.add(column.getName());
        }

        recommendations.add(new Recommendation(String.format(STATS_WITH_PARTITION_MSG, tableName, String.join(", ", columnNames))));
      }
    }

    return recommendations;
  }
}
