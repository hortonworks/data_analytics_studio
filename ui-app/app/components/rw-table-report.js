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
    classNames: ['rw-table-report'],
    heatTables: [],
    headerComponentNames: ["em-table-header-component"],
    footerComponentNames: ['em-table-footer-component'],

    enableSort: true,
    enableSearch: true,
    enableFaceting: true,
    recordCount: 25,

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
        enablePagination: false,
        enableFaceting: false,
        rowCount: this.get('recordCount'),
        table: this});
    }),

    columns: Ember.computed(function () {
      var parentComponent = this;
      return ColumnDefinition.make([
        {
           id: 'tableName',
           headerTitle: "TABLE",
           enableSort: true,
           contentPath: "tableName",
           pin:"left",
           cellComponentName: 'em-reports-rw-table', //saved-query-actions
           getCellContent: function (row) {
              return {
                parentComponent: parentComponent,
                table: row
              }
           }
        },{
            id: 'readCount',
            headerTitle: "Read Count",
            contentPath: "readCount",
            enableSort: true,
        },{
            id: 'writeCount',
            headerTitle: "Write Count",
            contentPath: "writeCount",
            enableSort: true,
        },{
            id: 'bytesRead',
            headerTitle: "Bytes Read",
            contentPath: "bytesRead",
            enableSort: true,
        },{
            id: 'recordsRead',
            headerTitle: "Records Read",
            contentPath: "recordsRead",
            enableSort: true,
        },{
            id: 'type',
            headerTitle: "Type",
            enableSort: false,
            contentPath: "type",
            enableSort: true,
        },{
            id: 'columns',
            headerTitle: "Columns Read",
            enableSort: false,
            contentPath: "columns",
            enableSort: true,
        },{
            id: 'buckets',
            headerTitle: "Buckets",
            contentPath: "buckets",
            enableColumnResize: false,
            enableSort: true,
        },{
            id: 'compression',
            headerTitle: "Compression",
            contentPath: "compression",
            enableColumnResize: false,
            enableSort: true,
        },{
            id: 'owner',
            headerTitle: "Owner",
            contentPath: "owner",
            enableColumnResize: false,
            enableSort: true,
        }])
    }),

    rows: Ember.computed('heatTables', function () {
        let heatTables = this.get('heatTables');
        var rows = [], recordIndex = 1;
        heatTables.forEach((heatTable, index) => {
             if(heatTable.tableId == this.get('selectedTableId')) {
                recordIndex = index;
             }
             let {tableId, tableName, readCount, writeCount, bytesRead, recordsRead, bytesWritten, recordsWritten, type } = heatTable;
             if(type.trim().toUpperCase() == 'EXTERNAL_TABLE'){
               type = 'External';     
             }else if (type.trim().toUpperCase() == 'MANAGED_TABLE'){
               type = 'Managed';   
             } else {
                type = type; 
             }
             rows.push({
                tableId, tableName, readCount, writeCount, bytesRead, recordsRead, bytesWritten, recordsWritten, type,
                columns: heatTable.columns.length,
                buckets: heatTable.metaInfo[4].propertyValue === -1 ? "Not Applicable":heatTable.metaInfo[4].propertyValue ,
                compression: heatTable.metaInfo[3].propertyValue,
                owner: heatTable.metaInfo[5].propertyValue
            });
        });
        this.set('tableDefinition.pageNum', parseInt(recordIndex/this.get('recordCount'))+1);
        return rows;
    }),

    highlightFirstRow(){
        this.$('.table-body-left > .table-column .table-cell').children().first().parent().addClass('highlight-column');
        this.$('.table-body > .table-scroll-body > .table-column').each((index, item)=>{
            $(item).children('.table-cell').first().addClass('highlight-column')
        })
    },

    didInsertElement() {
        this._super(...arguments);
        Ember.run.scheduleOnce('afterRender', this, function(){
          this.highlightSelectedQuery();
        });
    },

    didUpdateAttrs() {
        this._super(...arguments);
        Ember.run.scheduleOnce('afterRender', this, function(){
          this.highlightSelectedQuery();
        });
    },
    highlightSelectedQuery() {
        let elem = this.$('.table-body-left > .table-column .table-cell .active');
        if(!this.get('selectedTableId').length || !elem.length) {
          this.highlightFirstRow();
          return;
        }
        this._undoHighlightRows();
        this._highlightCurrentRow(elem);
    },
    _undoHighlightRows : function(){
        this.$('.table-body-left > .table-column .table-cell').each(function(index, item){
            $(item).removeClass('highlight-column');
        });
        this.$('.table-body > .table-scroll-body > .table-column .table-cell').each( (index, item) =>{
            $(item).removeClass('highlight-column');
        })
    },

    _highlightCurrentRow : function(targetElement){

       let rowIndex = 0;
       $(targetElement).parent().parent().addClass('highlight-column');

       this.$('.table-body-left > .table-column .table-cell').each(function(index, item){
           if($(item).attr('class').indexOf('highlight-column') > -1){
             rowIndex = index;
             return;
           }
       });

       this.$('.table-body > .table-scroll-body > .table-column').each( (index, item) =>{
         $($(item).children('.table-cell')[rowIndex]).addClass('highlight-column');
       });
    },

    click(event){
       if($(event.target).hasClass('table-name')){
        this._undoHighlightRows();
        this._highlightCurrentRow($(event.target));
       }
    },

    didRender(){
        this._super(...arguments);
    },

    actions:{
        openTableMetaInfo(id) {
          this.sendAction("openTableMetaInfo", id);
        },

        testHook(id){
            this.sendAction("testHook", id);
        },

        columnWidthChanged: function (width, columnDefinition, index) {
          var scrollColumns = this.get("scrollColumns");
          if(columnDefinition.get("pin") === "center") {
            scrollColumns.replace(index, 1, {
              definition: columnDefinition,
              width: width
            });
          }
        },

        scrollChange: function (scrollData) {
          this.set("dataProcessor.scrollData", scrollData);
        },

        scrollToColumn: function (definition) {
          var scrollColumns = this.get("scrollColumns"),
              scrollPosition = 0;

          scrollColumns.some(function (column) {
            if(column.definition === definition) {
              return true;
            }
            scrollPosition = scrollPosition + column.width;
          });

          this.$().find(".table-body").animate({
            scrollLeft: scrollPosition
          });
        },
    }

});
