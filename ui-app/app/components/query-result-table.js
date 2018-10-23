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

export default Ember.Component.extend({

  classNames: ['query-result-table', 'clearfix'],
  jobId: null,
  columnWidth: 50,
  maxColumnWidth: 300,
  queryResult: {'schema' :[], 'rows' :[]},

  elementsInserted: (function () {
      if(this.get("downloadResults") && this.get("downloadResultsInit") === 1) {
       this.send("openDownloadCsvModal");
      }
  }).on('didInsertElement'),
  columnFilterText: null,
  columnFilter: null,
  isExplainPlanFormatted: false,

  columnFilterDebounced: Ember.observer('columnFilterText', function() {
    Ember.run.debounce(this, () => {
      this.set('columnFilter', this.get('columnFilterText'));
    }, 500);
  }),

  columns: Ember.computed('queryResult', function() {
    let columnArr =[];

    this.get('queryResult').schema.forEach((column) => {
      let tempColumn = {};

      tempColumn['label'] = column[0];

      let localValuePath = column[0];
      tempColumn['valuePath'] = localValuePath.substring(localValuePath.lastIndexOf('.') +1 , localValuePath.length);
      this.setColumnWidth(tempColumn);
      columnArr.push(tempColumn);
    });
    return columnArr;
  }),

  rows: Ember.computed('queryResult','columns', function() {
    let rowArr = [], self = this;
    self.get("isExplainPlanFormatted", false);

    if(self.get('columns').length > 0) {
      self.get('queryResult').rows.forEach(function(row){
        var mylocalObject = {};
        self.get('columns').forEach(function(column, index){
          mylocalObject[self.get('columns')[index].valuePath] = row[index];
          self.setColumnWidth({valuePath:row[index]});
        });
        try {
            if(JSON.parse(mylocalObject.Explain)) {
              self.set("isExplainPlanFormatted", true);
              self.set("ExplainPlan", JSON.parse(mylocalObject.Explain));
            }
        } catch(error){
        }

        if(self.get("isDescribeCommand")) {
          //self.set("isExplainPlanFormatted", true);
          self.set("ExplainPlan", (mylocalObject));
        }
        rowArr.push(mylocalObject);
      });
      return rowArr;
    }
    return rowArr;
  }),

  filteredColumns: Ember.computed('columns', 'columnFilter', function() {
    if (!Ember.isEmpty(this.get('columnFilter'))) {
      return this.get('columns').filter((item) => item.label.indexOf(this.get('columnFilter')) > -1 );
    }
    this.set('widthPercentage', 100/this.get('columns').length+"%");
    return this.get('columns');
  }),
  showSaveHdfsModal:false,

  showDownloadCsvModal: false,

  isExportResultSuccessMessege:false,

  isSaveHdfsErrorMessege:false,

  setColumnWidth(tempColumn) {
    let maxColumnWidth = this.get('maxColumnWidth');
    let tempLength = tempColumn['valuePath'].length*9;
    if(this.get('columnWidth') < tempLength) {
      this.set('columnWidth', tempLength >= maxColumnWidth ? maxColumnWidth : tempLength);
    }
  },
  actions: {
    onScrolledToBottom() {
      //console.log('hook for INFINITE scroll');
    },

    onColumnClick(column) {
      console.log('column',column);
    },
    goNextPage(payloadTitle){
      this.sendAction('goNextPage', payloadTitle);
    },
    goPrevPage(payloadTitle){
      this.sendAction('goPrevPage', payloadTitle);
    },
    expandQueryResultPanel(){
      this.sendAction('expandQueryResultPanel');
    },

    openSaveHdfsModal(){
      this.set('showSaveHdfsModal',true);
      this.set('isExportResultSuccessMessege',false);
      this.set('isExportResultFailureMessege',false);
    },

    closeSaveHdfsModal(){
      this.set('showSaveHdfsModal',false);
      this.set('isExportResultSuccessMessege',false);
      this.set('isExportResultFailureMessege',false);
    },

    openDownloadCsvModal(){
      this.set('showDownloadCsvModal',true);
      this.set('isExportResultSuccessMessege',false);
      this.set('isExportResultFailureMessege',false);
    },

    closeDownloadCsvModal(){
      this.set('showDownloadCsvModal',false);
      this.set('isExportResultSuccessMessege',false);
      this.set('isExportResultFailureMessege',false);
    },

    saveToHDFS(jobId, pathName){
      console.log('saveToHDFS with jobId == ', jobId );
      console.log('saveToHDFS with pathName == ', pathName );
      this.sendAction('saveToHDFS', jobId,  pathName);
    },

    downloadAsCsv(jobId, pathName){
      console.log('downloadAsCsv with jobId == ', jobId );
      console.log('downloadAsCsv with pathName == ', pathName );
      this.sendAction('downloadAsCsv', jobId,  pathName);
    },

    showVisualExplain(){
      this.sendAction('showVisualExplain');
    },

    clearColumnsFilter() {
      this.set('columnFilterText');
    }

  }

});
