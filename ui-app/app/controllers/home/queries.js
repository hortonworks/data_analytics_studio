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

  queryParams: ["queryID", "dagID", "appID", "user", "requestUser",
      "tablesRead", "tablesWritten", "operationID", "queue"],
  queryID: "",
  dagID: "",
  appID: "",
  executionMode: "",
  user: "",
  requestUser: "",
  tablesRead: "",
  tablesWritten: "",
  operationID: "",
  queue: "",

  // Because pageNo is a query param added by table controller, and in the current design
  // we don't want page to be a query param as only the first page will be loaded first.
  pageNum: 1,

  breadcrumbs: [{
    text: "All Queries",
    routeName: "home.queries",
  }],

  moreAvailable: false,
  loadingMore: false,

  headerComponentNames: ['queries-page-search', 'table-controls', 'pagination-ui'],
  footerComponentNames: ['pagination-ui'],

  _definition: TableDefinition.create(),
  // Using computed, as observer won't fire if the property is not used
  definition: Ember.computed("queryID", "dagID", "appID", "user", "requestUser",
      "executionMode", "tablesRead", "tablesWritten", "operationID", "queue",
      "pageNum", "moreAvailable", "loadingMore", function () {

    var definition = this.get("_definition");

    definition.setProperties({
      queryID: this.get("queryID"),
      dagID: this.get("dagID"),
      appID: this.get("appID"),
      executionMode: this.get("executionMode"),
      user: this.get("user"),
      requestUser: this.get("requestUser"),
      tablesRead: this.get("tablesRead"),
      tablesWritten: this.get("tablesWritten"),
      operationID: this.get("operationID"),
      queue: this.get("queue"),

      pageNum: this.get("pageNum"),

      moreAvailable: this.get("moreAvailable"),
      loadingMore: this.get("loadingMore")
    });

    return definition;
  }),

  columns: ColumnDefinition.make([{
    id: 'entityID',
    headerTitle: 'Query ID',
    contentPath: 'entityID',
    cellComponentName: 'em-table-linked-cell',
    minWidth: "250px",
    getCellContent: function (row) {
      return {
        routeName: "query",
        model: row.get("entityID"),
        text: row.get("entityID")
      };
    }
  },{
    id: 'requestUser',
    headerTitle: 'User',
    contentPath: 'requestUser',
    minWidth: "100px",
  },{
    id: 'status',
    headerTitle: 'Status',
    contentPath: 'status',
    cellComponentName: 'em-table-status-cell',
    minWidth: "105px",
  },{
    id: 'queryText',
    headerTitle: 'Query',
    contentPath: 'queryText',
  },{
    id: 'dagID',
    headerTitle: 'DAG ID',
    contentPath: 'dag.firstObject.entityID',
    cellComponentName: 'em-table-linked-cell',
    minWidth: "250px",
    getCellContent: function (row) {
      return {
        routeName: "dag",
        model: row.get("dag.firstObject.entityID"),
        text: row.get("dag.firstObject.entityID")
      };
    }
  },{
    id: 'tablesRead',
    headerTitle: 'Tables Read',
    contentPath: 'tablesRead',
  },{
    id: 'tablesWritten',
    headerTitle: 'Tables Written',
    contentPath: 'tablesWritten',
  },{
    id: 'llapAppID',
    headerTitle: 'LLAP App ID',
    contentPath: 'llapAppID',
    minWidth: "250px",
  },{
    id: 'clientAddress',
    headerTitle: 'Client Address',
    contentPath: 'clientAddress',
    hiddenByDefault: true,
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
    contentPath: 'dag.firstObject.appID',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "app",
        model: row.get("dag.firstObject.appID"),
        text: row.get("dag.firstObject.appID")
      };
    }
  },{
    id: 'queue',
    headerTitle: 'Queue',
    contentPath: 'queue',
  },{
    id: 'executionMode',
    headerTitle: 'Execution Mode',
    contentPath: 'executionMode',
    minWidth: "100px",
  },{
    id: 'hiveAddress',
    headerTitle: 'Hive Server 2 Address',
    contentPath: 'hiveAddress',
    hiddenByDefault: true,
  },{
    id: 'instanceType',
    headerTitle: 'Client Type',
    contentPath: 'instanceType',
    minWidth: "100px",
  }]),

  getCounterColumns: function () {
    return [];
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
