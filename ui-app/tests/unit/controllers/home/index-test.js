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

moduleFor('controller:home/index', 'Unit | Controller | home/index', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  assert.expect(2 + 4 + 1 + 4 + 2 + 2);

  let controller = this.subject({
    initVisibleColumns: Ember.K,
    beforeSort: {bind: Ember.K},
    send: function (name, query) {
      assert.equal(name, "setBreadcrumbs");
      assert.ok(query);
    }
  });

  assert.ok(controller);
  assert.ok(controller.columns);
  assert.ok(controller.columns.length, 13);
  assert.ok(controller.getCounterColumns);

  assert.ok(controller.pageNum);

  assert.ok(controller.queryParams);
  assert.ok(controller.headerComponentNames);
  assert.equal(controller.headerComponentNames.length, 3);
  assert.equal(controller.footerComponentNames.length, 2);

  assert.ok(controller._definition);
  assert.ok(controller.definition);

  assert.ok(controller.actions.search);
  assert.ok(controller.actions.pageChanged);
});

test('queryParams test', function(assert) {
  let controller = this.subject({
        initVisibleColumns: Ember.K,
        beforeSort: {bind: Ember.K},
        send: Ember.K
      });

  // 11 New, 5 Inherited & 4 for backward compatibility
  assert.equal(controller.get("queryParams.length"), 7 + 5 + 4);
});

test('definition test', function(assert) {
  let controller = this.subject({
        initVisibleColumns: Ember.K,
        beforeSort: {bind: Ember.K},
        send: Ember.K
      }),
      definition = controller.get("definition"),
      testDAGName = "DAGName",
      testDAGID = "DAGID",
      testSubmitter = "Submitter",
      testStatus = "Status",
      testAppID = "AppID",
      testCallerID = "CallerID",
      testQueue = "Queue",
      testPageNum = 10,
      testMoreAvailable = true,
      testLoadingMore = true;

  assert.equal(definition.get("dagName"), "");
  assert.equal(definition.get("dagID"), "");
  assert.equal(definition.get("submitter"), "");
  assert.equal(definition.get("status"), "");
  assert.equal(definition.get("appID"), "");
  assert.equal(definition.get("callerID"), "");
  assert.equal(definition.get("queue"), "");

  assert.equal(definition.get("pageNum"), 1);

  assert.equal(definition.get("moreAvailable"), false);
  assert.equal(definition.get("loadingMore"), false);

  Ember.run(function () {
    controller.set("dagName", testDAGName);
    assert.equal(controller.get("definition.dagName"), testDAGName);

    controller.set("dagID", testDAGID);
    assert.equal(controller.get("definition.dagID"), testDAGID);

    controller.set("submitter", testSubmitter);
    assert.equal(controller.get("definition.submitter"), testSubmitter);

    controller.set("status", testStatus);
    assert.equal(controller.get("definition.status"), testStatus);

    controller.set("appID", testAppID);
    assert.equal(controller.get("definition.appID"), testAppID);

    controller.set("callerID", testCallerID);
    assert.equal(controller.get("definition.callerID"), testCallerID);

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
    initVisibleColumns: Ember.K,
    beforeSort: {bind: Ember.K},
    send: Ember.K
  }).get("breadcrumbs");

  assert.equal(breadcrumbs.length, 1);
  assert.equal(breadcrumbs[0].text, "All DAGs");
});