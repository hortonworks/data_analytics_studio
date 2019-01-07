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
export default function tableHeatmap(model, countType) {

  if (model.reports.length == 0) {
    model.tables = [];
  };

  var tempTables = [];
  model.reports.forEach((item, index) => {
    item.tables.forEach((item, index) => {
      tempTables.push(item);
    });
  });

  var tableGroups = tempTables.reduce(function (res, currentValue) {
    if (res.indexOf(currentValue.id) === -1) {
      res.push(currentValue.id);
    }
    return res;
  }, []).map(function (id) {
    return {
      id: id,
      readCount: tempTables.filter(function (_el) {
        return _el.id === id;
      }).map(function (_el) { return _el.readCount; }),
      writeCount: tempTables.filter(function (_el) {
        return _el.id === id;
      }).map(function (_el) { return _el.writeCount; }),
      bytesRead: tempTables.filter(function (_el) {
        return _el.id === id;
      }).map(function (_el) { return _el.bytesRead; }),
      recordsRead: tempTables.filter(function (_el) {
        return _el.id === id;
      }).map(function (_el) { return _el.recordsRead; }),
      bytesWritten: tempTables.filter(function (_el) {
        return _el.id === id;
      }).map(function (_el) { return _el.bytesWritten; }),
      recordsWritten: tempTables.filter(function (_el) {
      return _el.id === id;
      }).map(function (_el) { return _el.recordsWritten; }),
      columns: tempTables.filter(function (_el) {
        return _el.id === id;
      }).map(function (_el) {
        return _el.columns;
      })
    }
  });

  var flatTableGroups = [];

  tableGroups.forEach((item) => {
    var tempTbl = {};
    tempTbl.id = item.id;
    var tempColumnForTbl = [];

    item.columns.forEach((item1) => {
      item1.forEach((item2) => {
        tempColumnForTbl.push(item2);
      });
    });

    tempTbl.columns = getFlatColumns(tempColumnForTbl)
                        .sort(function (a, b) { return b[countType] - a[countType] });

    var totalReadCount = 0;
    item.readCount.forEach((item1) => {
      totalReadCount = totalReadCount + item1;
    });
    tempTbl.readCount = totalReadCount;

    var totalWriteCount = 0;
    item.writeCount.forEach((item1) => {
      totalWriteCount = totalWriteCount + item1;
    });
    tempTbl.writeCount = totalWriteCount;

    var totalBytesRead = 0;
    item.bytesRead.forEach((item1) => {
      totalBytesRead = totalBytesRead + item1;
    });
    tempTbl.bytesRead = totalBytesRead;

    var totalRecordsRead = 0;
    item.recordsRead.forEach((item1) => {
      totalRecordsRead = totalRecordsRead + item1;
    });
    tempTbl.recordsRead = totalRecordsRead;

    var totalBytesWritten = 0;
    item.bytesWritten.forEach((item1) => {
      totalBytesWritten = totalBytesWritten + item1;
    });
    tempTbl.bytesWritten = totalBytesWritten;

    var totalRecordsWritten = 0;
    item.recordsWritten.forEach((item1) => {
      totalRecordsWritten = totalRecordsWritten + item1;
    });
    tempTbl.recordsWritten = totalRecordsWritten;

    flatTableGroups.push(tempTbl);
  });


  var heatTables = [];

  model.tables.forEach((item) => {

    var tempHeatMap = {};

    var tableId = item.id;

    tempHeatMap.tableId = tableId;

    tempHeatMap.tableName = item.name;

    tempHeatMap.type = item.type || '';

    tempHeatMap.metaInfo = [];

    let serde = { propertyName: "serde Library", propertyValue: item.serde }
    tempHeatMap.metaInfo.push(serde);

    let inputFormat = { propertyName: "Input Format", propertyValue: item.inputFormat }
    tempHeatMap.metaInfo.push(inputFormat);

    let outputFormat = { propertyName: "Output Format", propertyValue: item.outputFormat }
    tempHeatMap.metaInfo.push(outputFormat);

    let compressed = { propertyName: "Compressed", propertyValue: item.compressed }
    tempHeatMap.metaInfo.push(compressed);

    let numBuckets = { propertyName: "Number of Buckets", propertyValue: item.numBuckets }
    tempHeatMap.metaInfo.push(numBuckets);

    let owner = { propertyName: "Owner", propertyValue: item.owner }
    tempHeatMap.metaInfo.push(owner);

    let createdAt = { propertyName: "Create Time", propertyValue: item.createdAt }
    tempHeatMap.metaInfo.push(createdAt);

    tempHeatMap.columnsInfo = item.columns;

    var table = flatTableGroups.filter(function (el) {
      return el.id == tableId;
    });


    if (!!table[0]) { // this makes sure the tables which are part of reports will only show.
      var tempColumns = [];
      table[0].columns.forEach((item) => {
        var columnId = item.id;
        var tempColumnHeatMap = {};


        tempHeatMap.columnsInfo.forEach((item1) => {
          if (columnId == item1.id) {
            tempColumnHeatMap.id = item1.id;
            tempColumnHeatMap.comment = item1.comment;
            tempColumnHeatMap.datatype = item1.datatype;
            tempColumnHeatMap.name = item1.name;
            tempColumnHeatMap.isPartitioned = item1.isPartitioned;
            tempColumnHeatMap.isPrimary = item1.isPrimary;
            tempColumnHeatMap.isSortKey = item1.isSortKey;
          }
        });

        tempColumnHeatMap.aggregationCount = item.aggregationCount || 0;
        tempColumnHeatMap.filterCount = item.filterCount || 0;
        tempColumnHeatMap.joinCount = item.joinCount || 0;
        tempColumnHeatMap.projectionCount = item.projectionCount || 0;
        tempColumns.push(tempColumnHeatMap);

      })

      tempHeatMap.readCount = table[0].readCount;
      tempHeatMap.writeCount = table[0].writeCount;
      tempHeatMap.bytesRead = table[0].bytesRead;
      tempHeatMap.recordsRead = table[0].recordsRead;
      tempHeatMap.bytesWritten = table[0].bytesWritten;
      tempHeatMap.recordsWritten = table[0].recordsWritten;
      tempHeatMap.columns = tempColumns;
      heatTables.push(tempHeatMap);
    }
  });

  return heatTables;

  function getFlatColumns(columns) {

    var columnGroups = columns.reduce(function (res, currentValue) {
      if (res.indexOf(currentValue.id) === -1) {
        res.push(currentValue.id);
      }
      return res;
    }, []).map(function (id) {
      return {
        id: id,
        projectionCount: columns.filter(function (_el) {
          return _el.id === id;
        }).map(function (_el) { return _el.projectionCount; }),
        aggregationCount: columns.filter(function (_el) {
          return _el.id === id;
        }).map(function (_el) { return _el.aggregationCount; }),
        filterCount: columns.filter(function (_el) {
          return _el.id === id;
        }).map(function (_el) { return _el.filterCount; }),
        joinCount: columns.filter(function (_el) {
          return _el.id === id;
        }).map(function (_el) { return _el.joinCount; })
      }
    });

    columnGroups = columnGroups.map((item) => {
      return {
        id: item.id,
        projectionCount: item.projectionCount.reduce((a, b) => a + b, 0),
        aggregationCount: item.aggregationCount.reduce((a, b) => a + b, 0),
        filterCount: item.filterCount.reduce((a, b) => a + b, 0),
        joinCount: item.joinCount.reduce((a, b) => a + b, 0)
      }
    })

    return columnGroups;
  }

}
