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

import TableDefinition from 'em-table/utils/table-definition';
import ColumnDefinition from 'em-table/utils/column-definition';

export default Ember.Component.extend({
   'recommendationList': ["You’ve used sales_data table in your query. This table is in plain text format. You can speed up your query by 30% if you convert it into a ORC file.",
    "The table region doesn’t have statistics, do you want to build statistics?",
    "You’ve used where sales_data.date>’2017-01-01’ in your query, instead, use sales_data.year>2017. Given the table is partitioned by year. This should speed up your query."
   ],

    headerComponentNames: ["bulk-report-table-header"],

    tableDefinition: TableDefinition.create({
        enablePagination: false,
        enableFaceting: true
    }),

    columns: ColumnDefinition.make([{
        id: 'database',
        headerTitle: "DATABASE",
        minWidth: "50px",
        enableSort: false,
        contentPath: "databaseName"
    },{
        id: 'tableName',
        headerTitle: "TABLE NAME",
        enableSort: false,
        contentPath: "tableName"
    },{
        id: 'size',
        headerTitle: "size",
        contentPath: "size",
        minWidth: "50px",
        enableSort: true,
    },{
        id: 'hasStat',
        headerTitle: "Has Stat",
        contentPath: "hasStat",
        minWidth: "50px",
        enableColumnResize: false,
        enableSort: false,
    },{
        id: 'Format',
        headerTitle: "Format",
        contentPath: "Format",
        minWidth: "50px",
        enableColumnResize: false,
        enableSort: false,
    },{
        id: 'Compress',
        headerTitle: "Compress",
        contentPath: "Compress",
        minWidth: "50px",
        enableColumnResize: false,
        enableSort: false,
    },{
         id: 'Copy',
         headerTitle: "Copy",
         contentPath: "Copy",
         minWidth: "50px",
         enableColumnResize: false,
         enableSort: false,
         cellComponentName: 'bulk-report-copy-column'
     }]),

    rows: Ember.computed(function () {
        var rows = [];
        for(var i = 0; i < 25; i++) {
            rows.push({
                databaseName: "Database " + Math.floor(i/5),
                tableName: "table " + i,
                size: i+1,
                hasStat: "stat"+i,
                Format: "Format"+i,
                Compress: "Compress"+i
            });
        }
        return rows;
    }),
    actions: {
     toggleFacets(){
      this.$(".table-panel-left, .storage-refine, #db_accordion_expand").toggleClass("hide");
      Ember.$(".bulk-action-header").toggleClass("bulk-table-header-full-width");
     }
    }

});
