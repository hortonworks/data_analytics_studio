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

import TableController from '../table';
import ColumnDefinition from 'em-table/utils/column-definition';
import TableDefinition from 'em-table/utils/table-definition';

export default TableController.extend({

  queryParams: ["dagName", "dagID", "submitter", "status", "appID", "callerID", "queue",
      "appid", "id", "user", "dag_name"],
  dagName: "",
  dagID: "",
  submitter: "",
  status: "",
  appID: "",
  callerID: "",
  queue: "",

  appid: "",
  id: "",
  user: "",
  dag_name: "",

  // Because pageNo is a query param added by table controller, and in the current design
  // we don't want page to be a query param as only the first page will be loaded first.
  pageNum: 1,

  breadcrumbs: [{
    text: "All DAGs",
    routeName: "home.index",
  }],

  moreAvailable: false,
  loadingMore: false,

  headerComponentNames: ['dags-page-search', 'table-controls', 'pagination-ui'],
  footerComponentNames: ['home-table-controls', 'pagination-ui'],

  _definition: TableDefinition.create(),
  // Using computed, as observer won't fire if the property is not used
  definition: Ember.computed("dagName", "dagID", "submitter", "status", "appID", "callerID", "queue",
      "pageNum", "moreAvailable", "loadingMore", function () {

    var definition = this.get("_definition");
    if (!this.get("appID")) {
      this.set("appID", this.get("appid"));
      this.set("appid", "");
    }
    if (!this.get("dagID")) {
      this.set("dagID", this.get("id"));
      this.set("id", "");
    }
    if (!this.get("submitter")) {
      this.set("submitter", this.get("user"));
      this.set("user", "");
    }
    if (!this.get("dagName")) {
      this.set("dagName", this.get("dag_name"));
      this.set("dag_name", "");
    }

    definition.setProperties({
      dagName: this.get("dagName"),
      dagID: this.get("dagID"),
      submitter: this.get("submitter"),
      status: this.get("status"),
      appID: this.get("appID"),
      callerID: this.get("callerID"),
      queue: this.get("queue"),

      pageNum: this.get("pageNum"),

      moreAvailable: this.get("moreAvailable"),
      loadingMore: this.get("loadingMore"),

      minRowsForFooter: 0
    });

    return definition;
  }),

  columns: ColumnDefinition.make([{
    id: 'name',
    headerTitle: 'Dag Name',
    contentPath: 'name',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "dag",
        model: row.get("entityID"),
        text: row.get("name")
      };
    }
  },{
    id: 'entityID',
    headerTitle: 'Id',
    contentPath: 'entityID'
  },{
    id: 'submitter',
    headerTitle: 'Submitter',
    contentPath: 'submitter'
  },{
    id: 'status',
    headerTitle: 'Status',
    contentPath: 'status',
    cellComponentName: 'em-table-status-cell',
    observePath: true
  },{
    id: 'progress',
    headerTitle: 'Progress',
    contentPath: 'progress',
    cellComponentName: 'em-table-progress-cell',
    observePath: true
  },{
    id: 'startTime',
    headerTitle: 'Start Time',
    contentPath: 'startTime',
    cellComponentName: 'date-formatter',
  },{
    id: 'endTime',
    headerTitle: 'End Time',
    contentPath: 'endTime',
    cellComponentName: 'date-formatter',
  },{
    id: 'duration',
    headerTitle: 'Duration',
    contentPath: 'duration',
    cellDefinition: {
      type: 'duration'
    }
  },{
    id: 'appID',
    headerTitle: 'Application Id',
    contentPath: 'appID',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "app",
        model: row.get("appID"),
        text: row.get("appID")
      };
    }
  },{
    id: 'queue',
    headerTitle: 'Queue',
    contentPath: 'queue',
    observePath: true,
    getCellContent: function (row) {
      var queueName = row.get("queue");
      if(!row.get("queueName") && row.get("app.queue")) {
        return {
          comment: "Queue name for this row was loaded separately, and will not be searchable!",
          content: queueName
        };
      }
      return queueName;
    }
  },{
    id: 'callerID',
    headerTitle: 'Caller ID',
    contentPath: 'callerID'
  },{
    id: 'callerContext',
    headerTitle: 'Caller Context',
    contentPath: 'callerContext'
  },{
    id: 'logs',
    headerTitle: 'Logs',
    contentPath: 'containerLogs',
    cellComponentName: "em-table-linked-cell",
    cellDefinition: {
      target: "_blank"
    }
  }]),

  getCounterColumns: function () {
    return this._super().concat(this.get('env.app.tables.defaultColumns.dagCounters'));
  },

  actions: {
    search: function (properties) {
      this.setProperties(properties);
    },
    pageChanged: function (pageNum) {
      this.set("pageNum", pageNum);
    },
  }

});
