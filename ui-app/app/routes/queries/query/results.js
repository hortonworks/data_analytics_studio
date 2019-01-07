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
import UILoggerMixin from '../../../mixins/ui-logger';

export default Ember.Route.extend(UILoggerMixin, {
  hiveQuery: Ember.inject.service('hive-query'),
  jobs: Ember.inject.service(),
  query: Ember.inject.service(),
  breadCrumb: null,
  columnLimitToShow: 1000,
  beforeModel() {
  },
  model(){
    return this.modelFor('queries.query');
  },

  setupController(controller, model){
    this._super(...arguments);

    model.set('lastResultRoute', ".results");

    if(!Ember.isEmpty(model.get('currentJobData'))){

      let jobId = model.get('currentJobData').job.id;
      this.controller.set('model', model);
      this.controller.set('jobId', jobId);
      if(model.get('downloadResults')) {
        this.controllerFor('queries.query').set('downloadResultsInit', (this.controllerFor('queries.query').get('downloadResultsInit')?this.controllerFor('queries.query').get('downloadResultsInit'):0)+1);
        this.controller.set('downloadResultsInit', this.controllerFor('queries.query').get('downloadResultsInit'));
      } else {
        this.controllerFor('queries.query').set('downloadResultsInit', 0);
        this.controller.set('downloadResultsInit', 0);
      }
      this.controller.set('payloadTitle',  model.get('currentJobData').job.title);
      this.controller.set('isQueryRunning', model.get('isQueryRunning'));
      this.controller.set('previousPage', model.get('previousPage'));
      this.controller.set('hasNext', model.get('hasNext'));
      this.controller.set('hasPrevious', model.get('hasPrevious'));
      this.controller.set('queryResult', model.get('queryResult'));
      this.controller.set('isDescribeCommand', model.get('isDescribeCommand'));
      this.controller.set('isExportResultSuccessMessege', false);
      this.controller.set('isExportResultFailureMessege', false);
      this.controller.set('showSaveHdfsModal', false);
      this.controller.set('showDownloadCsvModal', false);
      this.controller.set('hasJobAssociated', true);
      if(!!model.get('queryResult.rows').get('firstObject') && model.get('queryResult.rows').get('firstObject').length > this.get('columnLimitToShow')){
        this.controller.set('veryLargeData', true);
      } else{
        this.controller.set('veryLargeData', false);
      }
    } else {
      this.controller.set('hasJobAssociated', false);
    }
  },

  actions:{

    saveToHDFS(jobId, path){

      var self = this;

      console.log('saveToHDFS query route with jobId == ', jobId);
      console.log('saveToHDFS query route with path == ', path);

      this.get('query').saveToHDFS(jobId, path)
        .then((data) => {

          console.log('successfully saveToHDFS', data);
          this.get('controller').set('isExportResultSuccessMessege', true);
          this.get('controller').set('isExportResultFailureMessege', false);

          Ember.run.later(() => {
            this.get('controller').set('showSaveHdfsModal', false);
            this.get('logger').success('Successfully Saved to HDFS.');

          }, 2 * 1000);

        }, (error) => {

          console.log("Error encountered", error);
          this.get('controller').set('isExportResultFailureMessege', true);
          this.get('controller').set('isExportResultSuccessMessege', false);

          Ember.run.later(() => {
            this.get('controller').set('showSaveHdfsModal', false);
            this.get('logger').danger('Failed to save to HDFS.', this.extractError(error));
          }, 2 * 1000);


        });
    },


    downloadAsCsv(jobId, path){
      console.log('downloadAsCsv query route with jobId == ', jobId);
      console.log('downloadAsCsv query route with path == ', path);
      let win = window.open("", "_self");
      let downloadAsCsvUrl = this.get('query').downloadAsCsv(jobId, path) || '';

      this.get('hiveQuery').getToken()
        .then(tokenJson => {

          this.get('controller').set('showDownloadCsvModal', false);
          this.get('logger').success('Successfully downloaded as CSV.'); 

          let domain = window.location.protocol + "//" + window.location.host;
           let url = domain + downloadAsCsvUrl + '?xsrfToken=' + tokenJson.token;
           win.location.href = url;
           win.focus();
        }) ,(error =>{
          this.get('logger').danger('Failed to download csv.', this.extractError(error));
        })
    },
  }

});
