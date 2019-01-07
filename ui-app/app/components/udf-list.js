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

import DataProcessor from 'em-table/utils/data-processor';
import TableDefinition from 'em-table/utils/table-definition';
import ColumnDefinition from 'em-table/utils/column-definition';

export default Ember.Component.extend({

  classNames: ['udf-list'],


  enableSort: true,
  enableSearch: true,
  enableFaceting: true,

  definition: Ember.computed('tableDefinition', function () {
    return this.get('tableDefinition');
  }),

  scrollColumns: null,

  columnSelectorIsVisible: false,

  init: function () {
    this._super();
    this.set("scrollColumns", Ember.A([]));
  },

  _dataProcessor: null,
  dataProcessor: Ember.computed("definition", "dataLoader", function () {
    var dataProcessor = this.get("dataLoader") || this.get("_dataProcessor");

    if(!dataProcessor) {
      dataProcessor = DataProcessor.create();
      this.set("_dataProcessor", dataProcessor);
    }

    dataProcessor.set("tableDefinition", this.get("definition"));
    dataProcessor.set("scrollColumns", this.get("scrollColumns"));

    return dataProcessor;
  }),

  tableDefinition: Ember.computed(function(){
    return TableDefinition.create({
      minRowsForFooter: 0,
      enablePagination: true,
      rowCount:25,
      enableFaceting: false,
      table: this});
  }),

  columns: Ember.computed(function () {

  var parentComponent = this;

  return ColumnDefinition.make([
  {
    id: 'name',
    headerTitle: "name",
    enableSort: true,
    contentPath: "name"
  },{
    id: 'classname',
    headerTitle: "class name",
    enableSort: true,
    contentPath: "classname"
  },{
    id: 'owner',
    headerTitle: "Owner",
    enableSort: true,
    contentPath: "owner"
  }, {
    id: 'fileResource',
    headerTitle: "file Resource Name",
    enableSort: true,
    contentPath: "fileResourceName"
  }, {
    id: 'fileResource',
    headerTitle: "file Resource Path",
    enableSort: true,
    contentPath: "fileResourcePath"
  }, {
     id: 'Actions',
     headerTitle: "Actions",
     contentPath: "Actions",
     minWidth: "50px",
     enableColumnResize: false,
     enableSort: false,
     cellComponentName: 'udf-actions',
     getCellContent: function (row) {
        return {
          parentComponent: parentComponent,
          udf: row
        }
     }
   }])
  }),

  udflist: Ember.A(),

  rows: Ember.computed('udflist', function () {
    let udflist = this.get('udflist');
    var rows = [];
    udflist.forEach((udf)=>{
        let fsRes = this.get('fileResourceList').filterBy('id', udf.fileResource)[0];
        rows.push({
          id:   udf.id,
          name:  udf.name,
          classname:  udf.classname,
          owner:  udf.owner,
          fileResource:  udf.fileResource,
          fileResourceName:  fsRes.name,
          fileResourcePath:  fsRes.path,
          owner: udf.owner
        });
    })

    let rowsDecend = rows.sort(function(a, b) {
      return b.id - a.id;
    });

    return rowsDecend;
  }),

  actions: {
    columnWidthChanged: function (width, columnDefinition, index) {
      //do nothing.
    },
    scrollChange: function (scrollData) {
      //do nothing.
    }
  }

});
