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

moduleFor('route:single-am-pollster', 'Unit | Route | single am pollster', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.canPoll);
  assert.ok(route._loadedValueObserver);
});

test('canPoll test', function(assert) {
  let route = this.subject({
    polling: {
      resetPoll: function () {}
    },
    _canPollObserver: function () {}
  });

  assert.notOk(route.get("canPoll"));

  route.setProperties({
    polledRecords: {},
    loadedValue: {
      app: {
        isComplete: false
      },
      dag: undefined
    }
  });
  assert.ok(route.get("canPoll"), true, "Test 1");

  route.set("loadedValue.app.isComplete", true);
  assert.notOk(route.get("canPoll"), "Test 2");

  route.set("loadedValue.app.isComplete", undefined);
  assert.notOk(route.get("canPoll"), "Test 3");

  route.set("loadedValue.dag", Ember.Object.create({
    isComplete: false
  }));
  assert.ok(route.get("canPoll"), "Test 4");

  route.set("loadedValue.dag.isComplete", true);
  assert.notOk(route.get("canPoll"), "Test 5");

  route.set("loadedValue.dag", undefined);
  assert.notOk(route.get("canPoll"), "Test 6");

  route.set("loadedValue.app.isComplete", false);
  assert.ok(route.get("canPoll"), "Test 7");
});

test('_loadedValueObserver test', function(assert) {
  let route = this.subject({
    polling: {
      resetPoll: function () {}
    },
    _canPollObserver: function () {}
  }),
  loadedValue = Ember.Object.create();

  assert.equal(route.get("polledRecords"), null);

  route.set("loadedValue", loadedValue);
  assert.equal(route.get("polledRecords.0"), loadedValue);

  route.set("polledRecords", null);

  loadedValue.set("loadTime", 1);
  assert.equal(route.get("polledRecords.0"), loadedValue);
});
