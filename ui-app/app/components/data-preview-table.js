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

  queryResult: {'schema' :[], 'rows' :[]},

  jobId: null,
  query: Ember.inject.service(),
  jobs: Ember.inject.service(),
  store: Ember.inject.service(),

  columnFilterText: null,
  columnFilter: null,
  columnLimitToShow: 1000,
  isLoading: true,
  showSaveHdfsModal:false,
  showDownloadCsvModal: false,
  isExportResultSuccessMessege:false,
  isExportResultFailureMessege:false,
  isSaveHdfsErrorMessege:false,
  veryLargeData: false,
  init() {
    this._super(...arguments);
    var self = this;
    var tableName = this.get("selectedTable").get("name");
    var dbName = this.get("selectedDatabase");

    var queryStr = "select * from " + tableName + " limit 20";
    let payload = {
      "selectedDatabase":dbName,
      "title":"",
      "query": queryStr,
      "referrer":"job",
      "globalSettings":""
      };

    this.get('query').createJob(payload).then(function(data) {

      self.get('jobs').waitForJobToComplete(data.job.id, 2 * 1000, false)
        .then((status) => {

          let jobDetails = self.get("store").peekRecord('job', data.job.id);
          self.getJobResult(data, payload.title, jobDetails);

        }, (error) => {

          console.log('error', error);

        });
    }, function(error) {
      console.log(error);
    });
  },
  columnFilterDebounced: Ember.observer('columnFilterText', function() {
    Ember.run.debounce(this, () => {
      this.set('columnFilter', this.get('columnFilterText'));
    }, 500);
  }),
  columns: Ember.computed('queryResult', function() {
    let columnArr =[], self = this;
    if(this.get('queryResult')){
        this.get('queryResult').schema.forEach(function(column){
          self.set("isLoading", false);

          let tempColumn = {};

          tempColumn['label'] = column[0];

          let localValuePath = column[0];
          tempColumn['valuePath'] = localValuePath.substring(localValuePath.lastIndexOf('.') +1 , localValuePath.length);

          columnArr.push(tempColumn);
        });
    }
    
    return columnArr;
  }),

  rows: Ember.computed('queryResult','columns', function() {
    let rowArr = [], self = this;

    if(self.get('columns').length > 0) {
      self.get('queryResult').rows.forEach(function(row){
        var mylocalObject = {};
        self.get('columns').forEach(function(column, index){
          mylocalObject[self.get('columns')[index].valuePath] = row[index];
        });
        rowArr.push(mylocalObject);
      });
    }
    return rowArr;
  }),

  filteredColumns: Ember.computed('columns', 'columnFilter', function() {
    if (!Ember.isEmpty(this.get('columnFilter'))) {
      return this.get('columns').filter((item) => item.label.indexOf(this.get('columnFilter')) > -1 );
    }

    if(this.get('columns').length > this.get('columnLimitToShow')){
      this.set('veryLargeData', true);
    } else{
      this.set('veryLargeData', false);
    }

    return this.get('columns');
  }),

  getJobResult(data, payloadTitle, jobDetails){
    let jobId = data.job.id, self = this;

    this.set('jobId', jobId);

    this.get('query').getJob(jobId, true).then(function(data) {

      self.set("queryResult", data);

    }, function(error) {
      console.log('error' , error);
    });
  },

actions:{
  downloadAsCsv(jobId, path){
    let downloadAsCsvUrl = this.get('query').downloadAsCsv(jobId, path) || '';
    this.set('showDownloadCsvModal', false);
    window.open(downloadAsCsvUrl);
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
  }
}

});
