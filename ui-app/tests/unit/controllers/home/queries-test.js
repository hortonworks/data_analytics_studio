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
import { moduleFor, test } from 'ember-qunit';

moduleFor('controller:home/queries', 'Unit | Controller | home/queries', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  });

  assert.ok(controller);

  assert.ok(controller.queryParams);
  assert.equal(controller.queryParams.length, 9 + 5);

  assert.ok(controller.breadcrumbs);
  assert.ok(controller.headerComponentNames);
  assert.equal(controller.headerComponentNames.length, 3);
  assert.equal(controller.footerComponentNames.length, 1);

  assert.ok(controller.definition);
  assert.ok(controller.columns);
  assert.equal(controller.columns.length, 17);

  assert.ok(controller.getCounterColumns);

  assert.ok(controller.actions.search);
  assert.ok(controller.actions.pageChanged);

  assert.equal(controller.get("pageNum"), 1);
});

test('definition test', function(assert) {
  let controller = this.subject({
        initVisibleColumns: Ember.K,
        beforeSort: {bind: Ember.K},
        send: Ember.K
      }),
      definition = controller.get("definition"),

      testQueryID = "QueryID",
      testDagID = "DagID",
      testAppID = "AppID",
      testExecutionMode = "ExecutionMode",
      testUser = "User",
      testRequestUser = "RequestUser",
      testTablesRead = "TablesRead",
      testTablesWritten = "TablesWritten",
      testQueue = "queue",

      testPageNum = 10,
      testMoreAvailable = true,
      testLoadingMore = true;

  assert.equal(definition.get("queryID"), "");
  assert.equal(definition.get("dagID"), "");
  assert.equal(definition.get("appID"), "");
  assert.equal(definition.get("executionMode"), "");
  assert.equal(definition.get("user"), "");
  assert.equal(definition.get("requestUser"), "");
  assert.equal(definition.get("tablesRead"), "");
  assert.equal(definition.get("tablesWritten"), "");
  assert.equal(definition.get("queue"), "");

  assert.equal(definition.get("pageNum"), 1);

  assert.equal(definition.get("moreAvailable"), false);
  assert.equal(definition.get("loadingMore"), false);

  Ember.run(function () {
    controller.set("queryID", testQueryID);
    assert.equal(controller.get("definition.queryID"), testQueryID);

    controller.set("dagID", testDagID);
    assert.equal(controller.get("definition.dagID"), testDagID);

    controller.set("appID", testAppID);
    assert.equal(controller.get("definition.appID"), testAppID);

    controller.set("executionMode", testExecutionMode);
    assert.equal(controller.get("definition.executionMode"), testExecutionMode);

    controller.set("user", testUser);
    assert.equal(controller.get("definition.user"), testUser);

    controller.set("requestUser", testRequestUser);
    assert.equal(controller.get("definition.requestUser"), testRequestUser);

    controller.set("tablesRead", testTablesRead);
    assert.equal(controller.get("definition.tablesRead"), testTablesRead);

    controller.set("tablesWritten", testTablesWritten);
    assert.equal(controller.get("definition.tablesWritten"), testTablesWritten);

    controller.set("queue", testQueue);
    assert.equal(controller.get("definition.queue"), testQueue);

    controller.set("pageNum", testPageNum);
    assert.equal(controller.get("definition.pageNum"), testPageNum);

    controller.set("moreAvailable", testMoreAvailable);
    assert.equal(controller.get("definition.moreAvailable"), testMoreAvailable);

    controller.set("loadingMore", testLoadingMore);
    assert.equal(controller.get("definition.loadingMore"), testLoadingMore);
  });
});

test('breadcrumbs test', function(assert) {
  let breadcrumbs = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  }).get("breadcrumbs");

  assert.equal(breadcrumbs.length, 1);
  assert.equal(breadcrumbs[0].text, "All Queries");
});

test('getCounterColumns test', function(assert) {
  let getCounterColumns = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  }).get("getCounterColumns");

  assert.equal(getCounterColumns().length, 0);
});