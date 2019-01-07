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
import TableMetaRouter from './table-meta-router';
import tabs from '../../../../../configs/edit-table-tabs';
import UILoggerMixin from '../../../../../mixins/ui-logger';
import commons from '../../../../../mixins/commons';

export default TableMetaRouter.extend(UILoggerMixin, commons, {

  tableOperations: Ember.inject.service(),

  activate() {
    let tableController = this.controllerFor('databases.database.tables.table');
    this.set('existingTabs', tableController.get('tabs'));
    tableController.set('tabs', []);
  },

  deactivate() {
    let tableController = this.controllerFor('databases.database.tables.table');
    tableController.set('tabs', this.get('existingTabs'));
  },

  setupController(controller, model) {
    this._super(controller, model);
    this.logGA('/database/table/edit');
    controller.set('tabs', Ember.copy(tabs));
  },

  actions: {

    cancel() {
      this.transitionTo('databases.database.tables');
    },

    closeEditTableModal(){
      this.controller.set('hasError', false);
      this.controller.set('modalMessage');
      this.controller.set('showModal', false);
    },

    edit(settings) {
      this._modalStatus(true, 'Submitting request to edit table');
      this.get('tableOperations').editTable(settings).then((job) => {
        this._modalStatus(true, 'Waiting for the table edit job to complete');
        return this.get('tableOperations').waitForJobToComplete(job.get('id'), 5 * 1000);
      }).then((status) => {
        this._modalStatus(true, 'Successfully altered table'); //Add REPL dump info also here.
        this.get('logger').success(`Successfully altered table '${settings.table}'`);
        this._transitionToTables(); 
      }).catch((err) => {
        this.controller.set('hasError', true);
        this._modalStatus(true, `Failed to edit table '${settings.table}'`);
        this.get('logger').danger(`Failed to alter table '${settings.table}'`, this.extractError(err));
      });
    }

  },

  _modalStatus(status, message) {
    this.controller.set('showModal', status);
    if(status) {
      this.controller.set('modalMessage', message);
    }
  },

  _transitionToTables() {
    Ember.run.later(() => {
      this._modalStatus(false);
      this.send('refreshTableInfo');
      this.transitionTo('databases.database.tables.table');
    }, 2000);
  }


});
