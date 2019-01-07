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
  title: "Hive Queries",

  queryParams: {
    queryID: REFRESH,
    dagID: REFRESH,
    appID: REFRESH,
    executionMode: REFRESH,
    user: REFRESH,
    requestUser: REFRESH,
    tablesRead: REFRESH,
    tablesWritten: REFRESH,
    operationID: REFRESH,
    queue: REFRESH,

    rowCount: REFRESH
  },

  loaderQueryParams: {
    id: "queryID",
    dagID: "dagID",
    appID: "appID",
    executionMode: "executionMode",
    user: "user",
    requestuser: "requestUser",
    tablesRead: "tablesRead",
    tablesWritten: "tablesWritten",
    operationID: "operationID",
    queue: "queue",

    limit: "rowCount",
  },

  entityType: "hive-query",
  loaderNamespace: "queries",

  fromId: null,

  load: function (value, query, options) {
    var that = this;

    if(query.dagID) {
      return that.get("loader").queryRecord("dag", query.dagID).then(function (dag) {
        return that.load(value, {
          id: dag.get("callerID")
        }, options);
      }, function () {
        return [];
      });
    }
    else if(query.appID) {
      return that.get("loader").query("dag", {
        appID: query.appID,
        limit: query.limit
      }).then(function (dags) {
        return Ember.RSVP.all(dags.map(function (dag) {
          return that.get("loader").queryRecord("hive-query", dag.get("callerID"), options);
        }));
      }, function () {
        return [];
      });
    }

    return this._super(value, query, options).then(function (records) {
      return records.toArray();
    });
  },

  setupController: function (controller, model) {
    this._super(controller, model);
    Ember.run.later(this, "startCrumbBubble");
  },

  actions: {
    willTransition: function () {
      var loader = this.get("loader");
      loader.unloadAll("hive-query");
      this._super();
    },
  }
});
