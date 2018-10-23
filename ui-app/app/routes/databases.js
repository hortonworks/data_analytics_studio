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
import UILoggerMixin from '../mixins/ui-logger';
/*
import ENV from '/ui/config/environment';
*/
import ENV from './../config/environment';
import commons from '../mixins/commons';

export default Ember.Route.extend(UILoggerMixin, commons, {
  tableOperations: Ember.inject.service(),
  autoRefresh: Ember.inject.service(),
  breadCrumb: null,
  activate() {
    if(ENV.APP.SHOULD_AUTO_REFRESH_DATABASES) {
      this.get('autoRefresh').startDatabasesAutoRefresh(() => {
        console.log("Databases AutoRefresh started");
      }, this._databasesRefreshed.bind(this));
    }

  },

  deactivate() {
    if(ENV.APP.SHOULD_AUTO_REFRESH_DATABASES) {
      this.get('autoRefresh').stopDatabasesAutoRefresh();
    }
  },

  _databasesRefreshed() {
    let model = this.store.peekAll('database');
    if(this.controller) {
      console.log(model.get('length'));
      this.setupController(this.controller, model);
    }
  },
  beforeModel() {
    this.closeAutocompleteSuggestion();
    this.setActiveTab('databases');

  },
  model() {
    return this.store.findAll('database', {reload: true});
  },

  afterModel(model) {
    if (model.get('length') > 0) {
      this.selectDatabase(model);
      if (this.controller) {
        this.setupController(this.controller, model);
      }
    }
  },

  setupController(controller, model) {
    let sortedModel = model.sortBy('name');
    let selectedModel = sortedModel.filterBy('selected', true).get('firstObject');
    sortedModel.removeObject(selectedModel);

    if(selectedModel) {
        let finalList = [];
        finalList.pushObject(selectedModel);
        finalList.pushObjects(sortedModel);
        controller.set('model', finalList);
        controller.set('selectedDb', selectedModel.get('name'));
    }
  },

  selectDatabase(model) {
    let alreadySelected = model.findBy('selected', true);
    if (Ember.isEmpty(alreadySelected)) {
      // Check if params present
      let paramsForDatabase = this.paramsFor('databases.database');
      let toSelect = null;
      if (!Ember.isEmpty(paramsForDatabase.databaseId)) {
        toSelect = model.findBy('name', paramsForDatabase.databaseId);
      } else {
        // check if default database is present
        toSelect = model.findBy('name', 'default');
      }

      if (Ember.isEmpty(toSelect)) {
        let sortedModel = model.sortBy('name');
        toSelect = sortedModel.get('firstObject');
      }
      toSelect.set('selected', true);
    }
  },

  actions: {
    databaseSelected(database) {
      this.get('controller').set('selectedDb', database.get('name'));
      this.transitionTo('databases.database.tables', database.get('name'));
    },

    dropDatabase() {
      let databases = this.get('controller.model');
      let selectedModel = databases.filterBy('selected', true).get('firstObject');
      if (Ember.isEmpty(selectedModel)) {
        return;
      }

      this.get('controller').set('databaseToDelete', selectedModel);
      if (selectedModel.get('tables.length') > 0) {
        this.get('controller').set('databaseNotEmpty', true);
        console.log('database not empty');
        return;
      }
      this.get('controller').set('confirmDropDatabase', true);
    },

    notEmptyDialogClosed() {
      this.get('controller').set('databaseNotEmpty', false);
      this.get('controller').set('databaseToDelete', undefined);
    },

    databaseDropConfirmed() {
      console.log('drop confirmed');
      this.get('controller').set('confirmDropDatabase', false);
      this.logGA('DATABASE_DROP');
      this.controller.set('showDeleteDatabaseModal', true);
      this.controller.set('deleteDatabaseMessage', 'Submitting request to delete database');
      let databaseModel = this.controller.get('databaseToDelete');
      this.get('tableOperations').deleteDatabase(databaseModel)
        .then((job) => {
          this.controller.set('deleteDatabaseMessage', 'Waiting for the database to be deleted');
          this.get('tableOperations').waitForJobToComplete(job.get('id'), 5 * 1000)
            .then((status) => {
              this.controller.set('deleteDatabaseMessage', "Successfully deleted database");
              this.get('logger').success(`Successfully deleted database '${databaseModel.get('name')}'`);
              Ember.run.later(() => {
                this.store.unloadRecord(databaseModel);
                this.controller.set('showDeleteDatabaseModal', false);
                this.controller.set('deleteDatabaseMessage');
                this.replaceWith('databases');
                this.refresh();
              }, 2 * 1000);
            }, (error) => {
              this.get('logger').danger(`Failed to delete database '${databaseModel.get('name')}'`, this.extractError(error));
              Ember.run.later(() => {
                this.controller.set('showDeleteDatabaseModal', false);
                this.controller.set('deleteDatabaseMessage');
                this.replaceWith('databases');
                this.refresh();
              }, 1 * 1000);
            });
        }, (error) => {
          this.get('logger').danger(`Failed to delete database '${databaseModel.get('name')}'`, this.extractError(error));
          this.controller.set('showDeleteDatabaseModal', false);
        });
    },

    databaseDropDeclined() {
      console.log('drop declined');
      this.get('controller').set('confirmDropDatabase', false);
      this.get('controller').set('databaseToDelete', undefined);
    }
  }
});
