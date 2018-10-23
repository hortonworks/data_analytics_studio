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
import { moduleForModel, test } from 'ember-qunit';

moduleForModel('timeline', 'Unit | Model | timeline', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(!!model);

  assert.ok(model.needs);

  assert.ok(model.entityID);
  assert.ok(model.appID);
  assert.ok(model.app);

  assert.ok(model.atsStatus);
  assert.ok(model.status);
  assert.ok(model.progress);

  assert.ok(model._counterGroups);
  assert.ok(model.counterGroupsHash);
});

test('appID test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("entityID", "a_1_2_3");
    assert.equal(model.get("appID"), "application_1_2");
  });
});

test('status test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("atsStatus", "RUNNING");
    assert.equal(model.get("status"), "RUNNING");

    model.set("app", {
      status: "FAILED"
    });
    assert.equal(model.get("status"), "FAILED");
  });
});

test('progress test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("status", "RUNNING");
    assert.equal(model.get("progress"), null);

    model.set("status", "SUCCEEDED");
    assert.equal(model.get("progress"), 1);
  });
});

test('counterGroupsHash test', function(assert) {
  let model = this.subject(),
      testCounterGroup = [{
        counterGroupName: "group_1",
        counters: [{
          counterName: "counter_1_1",
          counterValue: "value_1_1"
        },{
          counterName: "counter_1_2",
          counterValue: "value_1_2"
        }]
      },{
        counterGroupName: "group_2",
        counters: [{
          counterName: "counter_2_1",
          counterValue: "value_2_1"
        },{
          counterName: "counter_2_2",
          counterValue: "value_2_2"
        }]
      }];

  Ember.run(function () {
    model.set("_counterGroups", testCounterGroup);
    assert.equal(model.get("counterGroupsHash.group_1.counter_1_1"), "value_1_1");
    assert.equal(model.get("counterGroupsHash.group_1.counter_1_2"), "value_1_2");
    assert.equal(model.get("counterGroupsHash.group_2.counter_2_1"), "value_2_1");
    assert.equal(model.get("counterGroupsHash.group_2.counter_2_2"), "value_2_2");
  });
});
