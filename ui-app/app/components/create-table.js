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
import Helper from '../configs/helpers';
import FileFormats from '../configs/file-format';
import commons from '../mixins/commons';
import { sanitize } from 'dom-purify';

export default Ember.Component.extend(commons, {
  init() {
    this._super(...arguments);
    this.logGA('TABLE_CREATE');
    let defaultFileFormat = FileFormats.findBy('default', true);
    this.set('columns', Ember.A());
    this.set('properties', []);
    this.set('settings', {
      fileFormat: { type: defaultFileFormat.name}
    });
    this.set('shouldAddBuckets', null);
    this.set('settingErrors', []);
  },

  didReceiveAttrs() {
    this.get('tabs').setEach('active', false);
    let firstTab = this.get('tabs.firstObject');
    firstTab.set('active', true);
  },

  actions: {
    activate(link) {
      console.log("Activate: ", link);
    },

    sanitizeTableName() {
      let sanitizedTableName = DOMPurify.sanitize(this.get('tableName'));
      this.set('tableName', sanitizedTableName);
    },

    create() {
      if (this.validate()) {
        this.sendAction('create', {
          name: this.get('tableName'),
          columns: this.get('columns'),
          settings: this.get('settings'),
          properties: this.get('properties')
        });
      }
    },

    cancel() {
      this.sendAction('cancel');
    }
  },

  validate() {
    if (!this.validateTableName()) {
      return false;
    }
    if (!(this.checkColumnsExists() &&
      this.checkColumnUniqueness() &&
      this.validateColumns() &&
      this.checkClusteringIfTransactional())) {
      this.selectTab("create.table.columns");
      return false;
    }

    if(!(this.validateNumBuckets())) {
      this.selectTab("create.table.advanced");
      return false;
    }

    if (!(this.validateTableProperties())) {
      this.selectTab("create.table.properties");
      return false;
    }
    return true;
  },
  validateTableName() {
    this.set('hasTableNameError');
    this.set('tableNameErrorText');

    if (Ember.isEmpty(this.get('tableName'))) {
      this.set('hasTableNameError', true);
      this.set('tableNameErrorText', 'Name cannot be empty');
      return false;
    }

    return true;
  },

  checkColumnsExists() {
    this.set('hasTableConfigurationError');
    this.set('tableConfigurationErrorText');
    if (this.get('columns.length') === 0) {
      this.set('hasTableConfigurationError', true);
      this.set('tableConfigurationErrorText', 'No columns configured. Add some column definitions.');
      return false;
    }
    return true;
  },

  checkColumnUniqueness() {
    let columnNames = [];
    for (let i = 0; i < this.get('columns.length'); i++) {
      let column = this.get('columns').objectAt(i);
      column.clearError();
      if (columnNames.indexOf(column.get('name')) === -1) {
        columnNames.pushObject(column.get('name'));
      } else {
        column.get('errors').push({type: 'name', error: 'Name should be unique'});
        return false;
      }
    }

    return true;
  },

  validateColumns() {
    for (let i = 0; i < this.get('columns.length'); i++) {
      let column = this.get('columns').objectAt(i);
      if (!column.validate()) {
        return false;
      }
    }
    return true;
  },

  checkClusteringIfTransactional() {

    if(!this.get('columns')){
      return false;
    }

    let clusteredColumns = this.get('columns').filterBy('isClustered', true);
    if (this.get('settings.transactional') && clusteredColumns.get('length') === 0) {
      this.set('hasTableConfigurationError', true);
      this.set('tableConfigurationErrorText', 'Table is marked as transactional but no clustered column defined. Add some clustered column definitions.');
      return false;
    }
    return true;
  },

  validateTableProperties() {
    for (let i = 0; i < this.get('properties.length'); i++) {
      let property = this.get('properties').objectAt(i);
      if (!property.validate()) {
        return false;
      }
    }
    return true;
  },

  validateNumBuckets() {
    let clusteredColumns = this.get('columns').filterBy('isClustered', true);


    function isNumBucketsPresentAndIsAnInteger(context) {
      return (Ember.isEmpty(context.get('settings.numBuckets')) ||
      !Helper.isInteger(context.get('settings.numBuckets')));
    }

    if(clusteredColumns.get('length') > 0 && isNumBucketsPresentAndIsAnInteger(this)) {
      this.get('settingErrors').pushObject({type: 'numBuckets', error: "Some columns are clustered, Number of buckets are required."});
      return false;
    }

    return true;
  },

  selectTab(link) {
    this.get('tabs').setEach('active', false);
    let selectedTab = this.get('tabs').findBy('link', link);
    if (!Ember.isEmpty(selectedTab)) {
      selectedTab.set('active', true);
    }
  }
});
