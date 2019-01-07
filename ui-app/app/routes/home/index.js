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

import ServerSideOpsRoute from '../server-side-ops';

const REFRESH = {refreshModel: true};

export default ServerSideOpsRoute.extend({
  title: "All DAGs",

  queryParams: {
    dagName: REFRESH,
    dagID: REFRESH,
    submitter: REFRESH,
    status: REFRESH,
    appID: REFRESH,
    callerID: REFRESH,
    queue: REFRESH,

    rowCount: REFRESH
  },

  loaderQueryParams: {
    dagName: "dagName",
    id: "dagID",
    user: "submitter",
    status: "status",
    appID: "appID",
    callerID: "callerID",
    queueName: "queue",

    limit: "rowCount",
  },

  entityType: "dag",
  loaderNamespace: "dags",

  visibleRecords: [],

  setupController: function (controller, model) {
    this._super(controller, model);
    Ember.run.later(this, "startCrumbBubble");
  },

  // Client side filtering to ensure that records are relevant after status correction
  filterRecords: function (records, query) {
    query = {
      name: query.dagName,
      entityID: query.id,
      submitter: query.submitter,
      status: query.status,
      appID: query.appID,
      callerID: query.callerID,
      queue: query.queueName
    };

    return records.filter(function (record) {
      for(var propName in query) {
        if(query[propName] && query[propName] !== record.get(propName)) {
          return false;
        }
      }
      return true;
    });
  },

  load: function (value, query, options) {
    var loader = this._super(value, query, options),
        that = this;
    return loader.then(function (records) {
      records = that.filterRecords(records, query);
      records.forEach(function (record) {
        if(record.get("status") === "RUNNING") {
          that.get("loader").loadNeed(record, "am", {reload: true}).catch(function () {
            if(!record.get("isDeleted")) {
              record.set("am", null);
            }
          });
        }
      });
      return records;
    });
  },

  actions: {
    willTransition: function () {
      var loader = this.get("loader");
      loader.unloadAll("dag");
      loader.unloadAll("ahs-app");
      this._super();
    },

    loadCounters: function () {
      var visibleRecords = this.get("visibleRecords").slice(),
          loader = this.get("loader");

      function loadInfoOfNextDAG() {
        if(visibleRecords.length) {
          loader.loadNeed(visibleRecords.shift(), "info").finally(loadInfoOfNextDAG);
        }
      }

      loadInfoOfNextDAG();
    },
    tableRowsChanged: function (records) {
      this.set("visibleRecords", records);
    }
  }
});
