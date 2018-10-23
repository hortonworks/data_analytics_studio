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
import commons from '../../mixins/commons';

export default Ember.Route.extend(commons, {
  breadCrumb: {
    title: 'UDF'
  },
  beforeModel() {
    this.closeAutocompleteSuggestion();
  },

  model() {
    return Ember.RSVP.hash({
      udfs: this.store.findAll('udf').then(udfs => udfs.toArray()),
      fileResources: this.store.findAll('file-resource').then(fileResources => fileResources.toArray()),
    });
  },

  store: Ember.inject.service(),

  fileResourceHash: null,

  fileResources: function (model) {

    let fileResourceList = Ember.A();
    let fileResourceHash = {};

    model.fileResources.forEach(x => {
      let localFileResource = {
        id: x.get('id'),
        name: x.get('name'),
        path: x.get('path'),
        owner: x.get('owner')
      };
      fileResourceList.push(localFileResource);
      fileResourceHash[x.get('id')] = { name: x.get('name'), path: x.get('path'), owner: x.get('owner') };
    });

    fileResourceList.push({ 'name': 'Add New File Resource', 'action': 'addNewFileResource' });
    this.set('fileResourceHash', fileResourceHash);

    return fileResourceList;

  },

  udfList: function (model) {
    let udfList = Ember.A();
    model.udfs.forEach(x => {
      let localUdf = {
        id: x.get('id'),
        classname: x.get('classname'),
        name: x.get('name'),
        owner: x.get('owner'),
        fileResource: x.get('fileResource'),
        fileResourceName: this.get('fileResourceHash')[x.get('fileResource')] ? this.get('fileResourceHash')[x.get('fileResource')].name : null,
        fileResourcePath: this.get('fileResourceHash')[x.get('fileResource')] ? this.get('fileResourceHash')[x.get('fileResource')].path : null,
      };
      udfList.push(localUdf);
    });
    return udfList;

  },

  setupController(controller, model) {
    this._super(...arguments);

    let fileResources = this.fileResources(model);
    controller.set('fileResources', fileResources);

    let udfList = this.udfList(model);
    controller.set('udflist', udfList);

    controller.set('showDeleteUdfModal', false);
    controller.set('udfId', null);
  },

  actions: {

    showRemoveUdfModal(udfId) {
      console.log('udfId', udfId);
      this.get('controller').set('showDeleteUdfModal', true);
      this.get('controller').set('udfId', udfId);
    },
    cancelUdf() {
      this.get('controller').set('showDeleteUdfModal', false);
      this.get('controller').set('udfId', null);
    },
    removeUdf() {
      this.logGA('UDF_DELETE');
      let udfId = this.get('controller').get('udfId');
      let record = this.get('store').peekRecord('udf', udfId);
      if (record) {
        record.destroyRecord().then(() => {
          let udfId = this.get('controller').get('udfId');
          let updatedUdflist = this.get('controller').get('udflist').filter((item) => {
            if (!(item.id == udfId)) {
              return item;
            }
          });
          this.get('controller').set('udflist', updatedUdflist);
          this.send('cancelUdf');
          this.transitionTo('udfs');
        })
      }
    }

  }
});
