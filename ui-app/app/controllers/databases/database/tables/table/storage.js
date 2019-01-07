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
import Ember from 'ember';
import ColumnDefinition from 'em-table/utils/column-definition';
import TableDefinition from 'em-table/utils/table-definition';

export default Ember.Controller.extend({
    tableDefinition: TableDefinition.create({
        enablePagination: false,
        enableFaceting: false,
        searchType: "regex",
        table: this
    }),

    columns: Ember.computed(function () {
      var parentComponent = this;
      return ColumnDefinition.make([{
        id: 'information',
        headerTitle: "INFORMATION",
        minWidth: "50px",
        enableSort: true,
        contentPath: "information"
    },{
        id: 'value',
        headerTitle: "VALUE",
        minWidth: "50px",
        enableSort: true,
        contentPath: "value",
        cellComponentName: 'em-table-storage',
            getCellContent: function (row) {
              return {
                parentComponent: parentComponent,
                row: row
              }
           }
    }])
    }),

    rows: Ember.computed('model', function () {

        var storageInfo = this.get('table').get('storageInfo');
        var rows = [];

        rows.push({information : 'SerDe Library', value: storageInfo.serdeLibrary});
        rows.push({information : 'Input Format', value: storageInfo.inputFormat});
        rows.push({information : 'Output Format', value: storageInfo.outputFormat});
        rows.push({information : 'Compressed', value: storageInfo.compressed});
        rows.push({information : 'Number of Buckets', value: storageInfo.numBuckets});
        rows.push({information : 'Bucket Columns', value: storageInfo.bucketCols});
        rows.push({information : 'Sort Columns', value: storageInfo.sortCols});
        rows.push({information : 'Parameters', value: storageInfo.parameters});
        return rows;
    })
})