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
import tabs from '../../../../configs/create-table-tabs';
import UILoggerMixin from '../../../../mixins/ui-logger';

export default Ember.Route.extend(UILoggerMixin, {
  tableOperations: Ember.inject.service(),

  setupController(controller, model) {
    this._super(controller, model);
    controller.set('tabs', Ember.copy(tabs));
  },

  // function is used in sub-classes
  /**
   * @param settings
   * @param shouldTransition : should transition to other route?
   * @returns {Promise.<TResult>|*}
   */
  createTable: function(settings, shouldTransition=false){
    this.controller.set('showCreateTableModal', true);
    this.controller.set('createTableMessage', 'Submitting request to create table');
    let databaseModel = this.controllerFor('databases.database').get('model');
    return this.get('tableOperations').submitCreateTable(databaseModel.get('name'), settings)
      .then((job) => {
        console.log('Created job: ', job.get('id'));
        this.controller.set('createTableMessage', 'Waiting for the table to be created');
        return this.get('tableOperations').waitForJobToComplete(job.get('id'), 5 * 1000)
          .then((status) => {
            this.controller.set('createTableMessage', "Successfully created table");
            this.get('logger').success(`Successfully created table '${settings.name}'`);
            Ember.run.later(() => {
            this.controller.set('showCreateTableModal', false);
            this.controller.set('createTableMessage');
            this._addTableToStoreLocally(databaseModel, settings.name);
            this._resetModelInTablesController(databaseModel.get('tables'));
              if(shouldTransition){
                this._transitionToCreatedTable(databaseModel.get('name'), settings.name);
              }
            }, 2 * 1000);
            return Ember.RSVP.Promise.resolve(job);
          }, (error) => {
            this.get('logger').danger(`Failed to create table '${settings.name}'`, this.extractError(error));
            this.controller.set('createTableMessage', `Failed to create table '${settings.name}'`); // Add REPL dump info here.
            this.controller.set('hasError', true);
            return Ember.RSVP.Promise.reject(error);
          });
      }, (error) => {
        this.get('logger').danger(`Failed to create table '${settings.name}'`, this.extractError(error));
        this.controller.set('createTableMessage', `Failed to create table '${settings.name}'`);
        this.controller.set('showCreateTableModal', true);
        this.controller.set('hasError', true);
        throw error;
      });
  },
  actions: {
    cancel() {
      let databaseController = this.controllerFor('databases.database');
      this.transitionTo('databases.database', databaseController.get('model'));
    },
    toggleCSVFormat: function() {
      console.log("inside new route toggleCSVFormat");
      this.toggleProperty('showCSVFormatInput');
    },

    create(settings) {
      // keep this a function call call only as the createTable function is used in sub-classes
      // Second parameter is by default false because Table will appear only after next REPL dump
      this.createTable(settings, false); 
    },
    closeCreateTableModal(){
      this.controller.set('hasError', false);
      this.controller.set('createTableMessage');
      this.controller.set('showCreateTableModal', false);
    }
  },

  _transitionToCreatedTable(database, table) {
    this.transitionTo('databases.database.tables.table', database, table);
  },

  _addTableToStoreLocally(database, table) {
    // Add only if it has not been added by the auto refresh
    let existingRecord = this.store.peekRecord('table', `${database.get('name')}/${table}`);
    if(Ember.isEmpty(existingRecord)) {
      this.store.createRecord('table', {
        id: `${database.get('name')}/${table}`,
        name: `${table}`,
        type: 'TABLE',
        selected: true,
        database: database
      });
    }
  },

  _resetModelInTablesController(tables) {
    let tablesController = this.controllerFor('databases.database.tables');
    tablesController.get('model').setEach('selected', false);
    tablesController.set('model', tables);
  }
});
