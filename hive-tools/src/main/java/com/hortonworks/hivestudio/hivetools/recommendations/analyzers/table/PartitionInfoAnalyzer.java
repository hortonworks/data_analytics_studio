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
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.hive.internal.dto.TableMeta;
import com.hortonworks.hivestudio.hivetools.recommendations.entities.Recommendation;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;

public class PartitionInfoAnalyzer implements TableAnalyzer {

  private static final Integer MEM_FACTOR = 1024 * 1024;

  @VisibleForTesting
  static final String TOO_MANY_SMALL_PARTITIONS_MSG = "Table %1$s has more than %2$s partitions and %1$s has median partition size of <%4$dGB. This is usually an indication that you have too many small partitions. Currently the table is partitioned on %3$s. You should consider changing partition key to a column that allows better grouping of your data. For example, use year instead of date as a partition key.";
  @VisibleForTesting
  static final String TOO_MANY_FILES_PER_PARTITIONS_MSG = "Table %s is having too many files per partition!";

  @VisibleForTesting
  Integer partitionCountThreshold;
  @VisibleForTesting
  Integer medianPartitionSizeThresholdInMb;
  @VisibleForTesting
  Integer fileCountThreshold;
  @VisibleForTesting
  Integer fileSizeThresholdInMb;

  @Inject
  public PartitionInfoAnalyzer(Configuration configuration) {
    this.partitionCountThreshold = Integer.parseInt(configuration.get("hivestudio.recommendations.partition.count.threshold", "1000"));
    this.medianPartitionSizeThresholdInMb = Integer.parseInt(configuration.get("hivestudio.recommendations.median.partition.size.threshold", "1024"));

    this.fileCountThreshold = Integer.parseInt(configuration.get("hivestudio.recommendations.file.count.threshold", "100"));
    this.fileSizeThresholdInMb = Integer.parseInt(configuration.get("hivestudio.recommendations.file.size.threshold", "15"));
  }

  public ArrayList<Recommendation> analyze(TableMeta table, Collection<TablePartitionInfo> partitions) {
    ArrayList<Recommendation> recommendations = new ArrayList<>();

    DescriptiveStatistics fileCountStats = new DescriptiveStatistics();
    DescriptiveStatistics partitionSizeStats = new DescriptiveStatistics();

    for (TablePartitionInfo partition : partitions) {
      fileCountStats.addValue(partition.getNumFiles());
      double dataSize = partition.getRawDataSize();
      partitionSizeStats.addValue(dataSize / MEM_FACTOR);
    }

    if(partitions.size() > partitionCountThreshold) {
      double medianPartitionSize = partitionSizeStats.getPercentile(50);
      if(medianPartitionSize < medianPartitionSizeThresholdInMb) {
        String columnNames = String.join(", ", table.getPartitionedColumnNames());
        String message = String.format(TOO_MANY_SMALL_PARTITIONS_MSG, table.getTable(), partitionCountThreshold, columnNames, medianPartitionSizeThresholdInMb / 1024);
        recommendations.add(new Recommendation(message));
      }
    }

    double medianFileCount = fileCountStats.getPercentile(50);
    if(medianFileCount > fileCountThreshold) {
      double averageFileSize = partitionSizeStats.getSum() / fileCountStats.getSum();
      if(averageFileSize < fileSizeThresholdInMb) {
        recommendations.add(new Recommendation(String.format(TOO_MANY_FILES_PER_PARTITIONS_MSG, table.getTable())));
      }
    }

    return recommendations;
  }

}
