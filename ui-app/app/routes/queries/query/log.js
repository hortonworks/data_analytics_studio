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

export default Ember.Route.extend({
  breadCrumb: null,

  jobs: Ember.inject.service(),
  query: Ember.inject.service(),

  model(){
    return this.modelFor('queries.query');
  },

  getLogsTillJobSuccess(jobId, model, controller){
    let self = this;
    this.get('jobs').waitForJobStatus(jobId)
      .then((status) => {
        console.log('status', status);
        if(status !== "succeeded"){

          self.fetchLogs(jobId).then((logFileContent) => {
            controller.set('logResults', logFileContent );
          }, (error) => {
            console.log('error',error);
          });

          Ember.run.later(() => {
            self.getLogsTillJobSuccess(jobId, model, controller);
          }, 5 * 1000);

        } else {

          self.fetchLogs(jobId).then((logFileContent) => {
            controller.set('logResults', logFileContent );
          }, (error) => {
            console.log('error',error);
          });

        }
      }, (error) => {
        console.log('error',error);
      });
  },

  fetchLogs(jobId){
    return new Ember.RSVP.Promise( (resolve, reject) => {
      this.get('query').retrieveQueryLog(jobId).then(function(fileContent) {
        resolve(fileContent);
      }, function(error){
        reject(error);
      });
    });
  },

  jobStatus(jobId){
    return new Ember.RSVP.Promise( (resolve, reject) => {
      this.get('jobs').waitForJobStatus(jobId).then(function(status) {
        resolve(status);
      }, function(error){
        reject(error);
      });
    });

  },

  setupController(controller, model){
    this._super(...arguments);

    model.set('lastResultRoute', ".log");

    if(!Ember.isEmpty(model.get('currentJobData'))){
      let jobId = model.get('currentJobData').job.id;
      this.controller.set('jobId', jobId);
      this.controller.set('logResults', model.get('logResults'));
      this.getLogsTillJobSuccess(jobId, model, controller);
      this.controller.set('hasJobAssociated', true);

    } else {
      this.controller.set('hasJobAssociated', false);
    }
  },

  actions:{

  }

});
