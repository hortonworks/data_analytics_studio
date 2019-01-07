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

export default Ember.Service.extend({
  tezViewURL: null,
  // replace used to avoid slash duplication by proxy
  tezApiURL: '/api/v1/views/TEZ'.replace(/^\/\//, '/'),
  tezURLPrefix: '/views/TEZ',
  tezDagPath: '?viewPath=/#/dag/',
  getTezViewInfo: function () {
    this.set('error', null);
    if (this.get('isTezViewAvailable')) {
      return;
    }

    var self = this;
    Ember.$.getJSON(this.get('tezApiURL'))
      .then(function (response) {
        self.getTezViewInstance(response);
      })
      .fail(function (response) {
        self.setTezViewError(response);
      });
  },

  getTezViewInstance: function (data) {
    var self = this;
    var url = this.get('tezApiURL') + '/versions/' + data.versions[0].ViewVersionInfo.version;

    Ember.$.getJSON(url)
      .then(function (response) {
        if (!response.instances.length) {
          self.setTezViewError(response);
          return;
        }

        self.set('isTezViewAvailable', true);

        var instance = response.instances[0].ViewInstanceInfo;
        self.setTezViewURL(instance);
      });
  },

  setTezViewURL: function (instance) {
    var url = "%@/%@/%@/".fmt(
      this.get('tezURLPrefix'),
      instance.version,
      instance.instance_name
    );
    this.set('tezViewURL', url);
  },
  setTezViewError: function (data) {
    // status: 404 => Tev View isn't deployed
    if (data.status && data.status === 404) {
      this.set('error', 'tez.errors.not.deployed');
      this.set('errorMsg', 'Tez view not deployed');
    } else if (data.instances && !data.instances.length) { // no instance created
      this.set('error', 'tez.errors.no.instance');
      this.set('errorMsg', 'Tez view instance not created');
    } else {
      this.set('error', 'error');
      this.set('errorMsg', 'Error occurred while dispaying TEZ UI');
    }
  },
  getTezViewData(){
    let tezData = {};
    if(this.get('error')){
      tezData.error = this.get('error');
      tezData.errorMsg = this.get('errorMsg');
    } else {
      tezData.tezUrl = this.get('tezViewURL') + this.get("tezDagPath");
    }
    return tezData;
  }
});
