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

  beforeModel() {
  },

  model(){
    return this.modelFor('queries.query');
  },

  setupController(controller, model){
    this._super(...arguments);
    model.set('lastResultRoute', ".visual-explain");

    if(!Ember.isEmpty(model.get('currentJobData'))) {
      let jobId = model.get('currentJobData').job.id;
      this.controller.set('jobId', jobId);
      this.controller.set('payloadTitle',  model.get('currentJobData').job.title);
      this.controller.set('isQueryRunning', model.get('isQueryRunning'));
      try {
        if(!Ember.isEmpty(JSON.parse(model.get('queryResult').rows[0][0])['STAGE PLANS'])){
          this.controller.set('visualExplainJson', model.get('queryResult').rows[0][0]);
        }
      }catch(error) { }
      this.controller.set('hasJobAssociated', true);
    } else {
      this.controller.set('hasJobAssociated', false);
    }
  },

  actions:{

  }

});
