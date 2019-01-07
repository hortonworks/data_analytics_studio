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
import ENV from './../../../config/environment';

import UILoggerMixin from '../../../mixins/ui-logger';

export default Ember.Route.extend(UILoggerMixin, {
  breadCrumb: null,
  autoRefresh: Ember.inject.service(),
  autoRefreshReplInfo: Ember.inject.service(),
  query: Ember.inject.service(),
  jobs: Ember.inject.service(),
  queryResult : undefined,
  activate() {
    if(ENV.APP.SHOULD_AUTO_REFRESH_TABLES) {
      let selectedDatabase = this.modelFor('databases.database');
      this.get('autoRefresh').startTablesAutoRefresh(selectedDatabase.get('name'),
        this.tableRefreshStarting.bind(this), this.tableRefreshed.bind(this));
    }

    if(ENV.APP.SHOULD_AUTO_REFRESH_REPL_INFO) {
      this.get('autoRefreshReplInfo').startReplAutoRefresh(() => {
      }, this._replInfoRefreshed.bind(this), 0);
    }

  },

  _replInfoRefreshed(){
    let timeSinceLastUpdate = this.get('autoRefreshReplInfo').getTimeSinceLastUpdate();
    this.get('controller').set('timeSinceLastUpdate',  timeSinceLastUpdate);
  },

  deactivate() {
    if(ENV.APP.SHOULD_AUTO_REFRESH_TABLES) {
      this.get('autoRefresh').stopTablesAutoRefresh(this.controller.get('database.name'));
    }

    if(ENV.APP.SHOULD_AUTO_REFRESH_REPL_INFO) {
      this.get('autoRefreshReplInfo').stopReplAutoRefresh();
    }  
  },

  tableRefreshStarting(databaseName) {
    this.controller.set('tableRefreshing', true);
  },

  tableRefreshed(databaseName, deletedTablesCount) {
    this.controller.set('tableRefreshing', false);
    let currentTablesForDatabase = this.store.peekAll('table').filterBy('database.name', databaseName);
    let paramsForTable = this.paramsFor('databases.database.tables.table');
    let currentTableNamesForDatabase = currentTablesForDatabase.mapBy('name');
    if (currentTableNamesForDatabase.length <= 0  || !currentTableNamesForDatabase.contains(paramsForTable.name)) {
      if(deletedTablesCount !== 0) {
        this.get('logger').info(`Current selected table '${paramsForTable.name}' has been deleted from Hive Server. Transitioning out.`);
        this.transitionTo('databases.database', databaseName);
        return;
      }
    }
    if(currentTablesForDatabase.get('length') > 0) {
      this.selectTable(currentTablesForDatabase);
      this.controller.set('model', currentTablesForDatabase.sortBy('name'));
    }
  },

  model() {
    let selectedDatabase = this.modelFor('databases.database');
    return this.store.query('table', {databaseId: selectedDatabase.get('name')});
  },

  afterModel(model) {
    if (model.get('length') > 0) {
      this.selectTable(model);
    }
  },
  selectTable(model) {
    let sortedModel = model.sortBy('name');
    let alreadySelected = sortedModel.findBy('selected', true);
    if(this.get("controller")) {
      this.get("controller").set('selectedTable', model.sortBy('name').findBy('selected', true));
    }
    if (Ember.isEmpty(alreadySelected)) {
      let paramsForTable = this.paramsFor('databases.database.tables.table');
      let toSelect = null;
      if (!Ember.isEmpty(paramsForTable.name)) {
        toSelect = sortedModel.findBy('name', paramsForTable.name);
      } else {
        toSelect = sortedModel.get('firstObject');
      }
      toSelect.set('selected', true);
    }
  },

  setupController(controller, model) {
    this._super(...arguments);
    let sortedModel = model.sortBy('name');
    controller.set('model', sortedModel);
    
    controller.set('databases', this.modelFor('databases'));
    let selectedDatabase = this.modelFor('databases.database');
    controller.set('selectedDb', {"name" : selectedDatabase.get("name"), id : selectedDatabase.get("name") });
    controller.set('database', selectedDatabase);
    controller.set('selectedTable', model.sortBy('name').findBy('selected', true));
  },


  actions: {
    tableSelected(table) {
      let tables = this.controllerFor('databases.database.tables').get('model');
      tables.forEach((table) => {
        table.set('selected', false);
      });
      table.set('selected', true);
      this.get("controller").set('selectedTable',table);
      this.transitionTo('databases.database.tables.table', table.get('name'));
    },
    refreshTable() {
      let databaseName = this.controller.get('database.name');
      this.get('autoRefresh').refreshTables(databaseName, this.tableRefreshStarting.bind(this), this.tableRefreshed.bind(this), true);
    }
  }
});
