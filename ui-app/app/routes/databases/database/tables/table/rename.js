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
import UILoggerMixin from '../../../../../mixins/ui-logger';
import commons from '../../../../../mixins/commons';

export default TableMetaRouter.extend(UILoggerMixin, commons, {

  tableOperations: Ember.inject.service(),
  beforeModel() {
    this.logGA('/database/table/rename');
  },
  activate() {
    let tableController = this.controllerFor('databases.database.tables.table');
    this.set('existingTabs', tableController.get('tabs'));
    tableController.set('tabs', []);
  },

  deactivate() {
    let tableController = this.controllerFor('databases.database.tables.table');
    tableController.set('tabs', this.get('existingTabs'));
  },

  actions: {
    cancel() {
      this.transitionTo('databases.database.tables');
    },

    rename(newTableName) {
      let tableName = this.controller.get('table.table');
      let databaseName = this.controller.get('table.database');
      this._renameTo(newTableName, tableName, databaseName);
    }
  },

  _renameTo(newTableName, oldTableName, databaseName) {
    this._modalStatus(true, 'Submitting request to rename table');
    this.get('tableOperations').renameTable(databaseName, newTableName, oldTableName).then((job) => {
      this._modalStatus(true, 'Waiting for the table to be renamed');
      return this.get('tableOperations').waitForJobToComplete(job.get('id'), 5 * 1000);
    }).then((status) => {
      this._modalStatus(true, 'Successfully renamed table');
      this.get('logger').success(`Successfully renamed table '${oldTableName}' to '${newTableName}'`);
      this._transitionToTables();
    }).catch((err) => {
      this._modalStatus(false, 'Failed to rename table');
      this.get('logger').danger(`Failed to rename table '${oldTableName}' to '${newTableName}'`, this.extractError(err));
    });
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
      this.transitionTo('databases');
    }, 2000);
  }


});
