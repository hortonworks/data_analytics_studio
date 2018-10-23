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
import UILoggerMixin from '../../../../mixins/ui-logger';
import commons from '../../../../mixins/commons';

export default Ember.Route.extend(UILoggerMixin, commons, {

  tableOperations: Ember.inject.service(),
  beforeModel() {
    this.logGA('DATABASE_NEW');
  },

  actions: {
    cancel() {
      this.transitionTo('databases');
    },

    create(newDatabaseName) {
      this._createDatabase(newDatabaseName);
    }
  },

  _createDatabase(newDatabaseName) {
    this._modalStatus(true, 'Submitting request to create database');
    this.get('tableOperations').createDatabase(newDatabaseName).then((job) => {
      this._modalStatus(true, 'Waiting for the database to be created');
      return this.get('tableOperations').waitForJobToComplete(job.get('id'), 5 * 1000);
    }).then((status) => {
      this._modalStatus(true, 'Successfully created database');
      this._transitionToDatabases(newDatabaseName);
      this.get('logger').success(`Successfully created database '${newDatabaseName}'`);
    }).catch((err) => {
      this._modalStatus(false);
      this.get('logger').danger(`Failed to create database '${newDatabaseName}'`, this.extractError(err));
    });
  },

  _modalStatus(status, message) {
    this.controller.set('showModal', status);
    if(status) {
      this.controller.set('modalMessage', message);
    }
  },

  _transitionToDatabases(databaseName) {
    Ember.run.later(() => {
      this._modalStatus(false);
      this.transitionTo('databases');
    }, 2000);
  }

});
