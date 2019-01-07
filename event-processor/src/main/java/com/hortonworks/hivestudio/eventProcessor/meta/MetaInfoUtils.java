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
package com.hortonworks.hivestudio.eventProcessor.meta;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.ParsedTableType;
import com.hortonworks.hivestudio.common.entities.SortOrder;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.util.Pair;
import com.hortonworks.hivestudio.common.util.ParserUtils;
import com.hortonworks.hivestudio.eventProcessor.configuration.Constants;

import lombok.extern.slf4j.Slf4j;

@Singleton
@Slf4j
public class MetaInfoUtils {
  public static final String NOT_COMPRESSED = "NO";
  private ObjectMapper objectMapper;

  @Inject
  public MetaInfoUtils(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public Integer convertNoOfBuckets(String numBuckets) {
    try {
      return Integer.parseInt(numBuckets);
    } catch (Exception e) {
      return null;
    }
  }

  public Boolean convertCompressed(String compressed) {
    if (null == compressed)
      return null;

    return !compressed.equalsIgnoreCase(NOT_COMPRESSED);
  }

  public Date convertStringToDate(String date) {
    if (null == date) {
      return null;
    }

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
    try {
      return new Date(simpleDateFormat.parse(date).getTime());
    } catch (ParseException e) {
      log.error("Error occurred while parsing date : {}", date);
    }
    return null;
  }

  /**
   * converts from FieldSchema to Column entity.
   * ignores the IsPartitioned, setLastUpdatedAt and table field. which needs to be set manually
   *
   * @param column
   * @param table
   * @return
   */
  @VisibleForTesting
  Column fieldSchemaToColumn(FieldSchema column, Integer position, Date updatedTime, Table table, boolean isBucketted,
                             SortOrder sortOrder, boolean isPartitioned) {
    Column newColumn = new Column();
    newColumn.setCreationSource(CreationSource.REPLICATION);
    newColumn.setComment(column.getComment());
    newColumn.setColumnPosition(position);

    String type = column.getType().toLowerCase();
    List<String> typePrecisionScale = ParserUtils.parseColumnDataType(type);
    String datatype = typePrecisionScale.get(0);
    String precisionString = typePrecisionScale.get(1);
    String scaleString = typePrecisionScale.get(2);
    Integer precision = !Strings.isNullOrEmpty(precisionString) ? Integer.valueOf(precisionString.trim()) : null;
    Integer scale = !Strings.isNullOrEmpty(scaleString) ? Integer.valueOf(scaleString.trim()) : null;

    newColumn.setDatatype(datatype);
    newColumn.setPrecision(precision);
    newColumn.setScale(scale);
    //TODO : need to check and verify the primary constraint with specific version of hive.
//    builder.isPrimary()
    newColumn.setDropped(Boolean.FALSE);
    newColumn.setDroppedAt(null);
    // TODO: isSortKey to be removed
//    builder.isSortKey()
    newColumn.setName(column.getName().toLowerCase());
    newColumn.setCreateTime(updatedTime);
    newColumn.setLastUpdatedAt(updatedTime);
    newColumn.setTableId(table.getId());
    // TODO : how to get the primary columns
    newColumn.setIsPrimary(false);

    newColumn.setIsClustered(isBucketted);
    newColumn.setSortOrderEnum(sortOrder);
    newColumn.setIsPartitioned(isPartitioned);
//    TODO: don't know the meaning or use of this field. Remove it if not in use.
    newColumn.setIsSortKey(false);

    return newColumn;
  }

  public Pair<Table, Collection<Column>> convertHiveTableToHSTable(org.apache.hadoop.hive.metastore.api.Table hiveTable, Database database, Date updatedTime) {
    log.info("Converting the hive table to HS table representation for table : {}.{}", database.getName(), hiveTable.getTableName());
    Table table = convertHiveTableToHSTableWithoutColumns(hiveTable, database, updatedTime);

    List<FieldSchema> normalColumns = hiveTable.getSd().getCols();
    Set<String> bucketColsSet = new HashSet<>();
    List<String> bucketCols = hiveTable.getSd().getBucketCols();
    if (null != bucketCols) {
      bucketColsSet.addAll(bucketCols);
    }

    Map<String, Integer> sortColsMap = new HashMap<>();
    List<Order> sortCols = hiveTable.getSd().getSortCols();
    if (null != sortCols) {
      sortCols.forEach(order -> sortColsMap.put(order.getCol(), order.getOrder()));
    }

    AtomicInteger index = new AtomicInteger();
    Map<String, Column> columnsList = normalColumns.stream()
        .map(column -> fieldSchemaToColumn(column, index.getAndIncrement(), updatedTime, table, bucketColsSet.contains(column.getName()),
            getSortOrder(column.getName(), sortColsMap), false))
        .collect(Collectors.toMap(Column::getName, Function.identity()));

    List<FieldSchema> partitionedColumns = hiveTable.getPartitionKeys();
    if (null != partitionedColumns) {
      Map<String, Column> partitionedColumnList = partitionedColumns.stream()
          .map(column -> fieldSchemaToColumn(column, index.getAndIncrement(), updatedTime, table, bucketColsSet.contains(column.getName()),
              getSortOrder(column.getName(), sortColsMap), true))
          .collect(Collectors.toMap(Column::getName, Function.identity()));
      columnsList.putAll(partitionedColumnList);
    }

    Map<String, String> parameters = hiveTable.getParameters();
    if (null != parameters) {
      ObjectNode props = objectMapper.convertValue(parameters, ObjectNode.class);
      table.setProperties(props);
    }

    return new Pair<>(table, columnsList.values());
  }

  private SortOrder getSortOrder(String colName, Map<String, Integer> sortColsMap) {
    return sortColsMap.containsKey(colName)
        ? SortOrder.values()[sortColsMap.get(colName)]
        : SortOrder.NONE;
  }

  private Table convertHiveTableToHSTableWithoutColumns(
      org.apache.hadoop.hive.metastore.api.Table hiveTable, Database database, Date updatedTime) {
    Table newTable = new Table();
    newTable.setCreationSource(CreationSource.REPLICATION);
    newTable.setName(hiveTable.getTableName().toLowerCase());
    newTable.setDbId(database.getId());

    newTable.setComment(hiveTable.getParameters().get(Constants.TABLE_COMMENT_PROPERTY));
    newTable.setTableType(hiveTable.getTableType());
    newTable.setParsedTableType(ParsedTableType.NORMAL);
    newTable.setCreateTime(convertHiveCreateTimeIntToDate(hiveTable.getCreateTime()));
    newTable.setOwner(hiveTable.getOwner());

    newTable.setDropped(Boolean.FALSE);
    newTable.setDroppedAt(null);

    StorageDescriptor sd = hiveTable.getSd();
    if (null != sd) {
      newTable.setInputFormat(sd.getInputFormat());
      newTable.setOutputFormat(sd.getOutputFormat());
      newTable.setSerde(sd.getSerdeInfo().getSerializationLib());
      newTable.setNumBuckets(sd.getNumBuckets());
      newTable.setLocation(sd.getLocation());
      newTable.setCompressed(sd.isCompressed());
      if (sd.getParameters() != null) {
        ObjectNode params = objectMapper.convertValue(sd.getParameters(), ObjectNode.class);
        newTable.setStorageParameters(params);
      }
    }

    newTable.setLastUpdatedAt(updatedTime);
    newTable.setRetention(hiveTable.getRetention());
    return newTable;
  }

  private Date convertHiveCreateTimeIntToDate(int createTime) {
    // time in int means it is in seconds and not in milliseconds
    return new Date(createTime * 1000l);
  }

  public HiveConf createHiveConfs(Configuration hadoopConfiguration) {
    return new HiveConf(hadoopConfiguration, this.getClass());
  }
}
