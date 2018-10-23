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

moduleFor('controller:dag/swimlane', 'Unit | Controller | dag/swimlane', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    beforeSort: {bind: Ember.K},
    initVisibleColumns: Ember.K,
    getCounterColumns: function () {
      return [];
    }
  });

  assert.ok(controller);
  assert.ok(controller.zoom);
  assert.ok(controller.breadcrumbs);
  assert.ok(controller.columns);
  assert.equal(controller.columns.length, 13);
  assert.ok(controller.processes);

  assert.ok(controller.dataAvailable);

  assert.ok(controller.actions.toggleFullscreen);
  assert.ok(controller.actions.click);
});

test('Processes test', function(assert) {

  var vertices = [Ember.Object.create({
    name: "v1"
  }), Ember.Object.create({
    name: "v2"
  }), Ember.Object.create({
    name: "v3"
  }), Ember.Object.create({
    name: "v4"
  })];
  vertices.firstObject = {
    dag: {
      edges: [{
        inputVertexName: "v1",
        outputVertexName: "v3"
      }, {
        inputVertexName: "v2",
        outputVertexName: "v3"
      }, {
        inputVertexName: "v3",
        outputVertexName: "v4"
      }]
    }
  };

  let controller = this.subject({
    send: Ember.K,
    beforeSort: {bind: Ember.K},
    initVisibleColumns: Ember.K,
    getCounterColumns: function () {
      return [];
    },
    model: vertices
  });

  var processes = controller.get("processes");

  assert.equal(processes[2].blockers[0].vertex, vertices[0]);
  assert.equal(processes[2].blockers[1].vertex, vertices[1]);
  assert.equal(processes[3].blockers[0].vertex, vertices[2]);
});

test('dataAvailable test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    beforeSort: {bind: Ember.K},
    initVisibleColumns: Ember.K,
    getCounterColumns: function () {
      return [];
    }
  }),
  dag = Ember.Object.create(),
  vertex = Ember.Object.create({
    dag: dag
  });

  assert.equal(controller.get("dataAvailable"), true, "No DAG or vertex");

  controller.set("model", Ember.Object.create({
    firstObject: vertex
  }));
  assert.equal(controller.get("dataAvailable"), false, "With vertex & dag but no amWsVersion");

  dag.set("isComplete", true);
  assert.equal(controller.get("dataAvailable"), true, "Complete DAG");
  dag.set("isComplete", false);

  dag.set("amWsVersion", 1);
  assert.equal(controller.get("dataAvailable"), false, "With vertex & dag but amWsVersion=1");

  dag.set("amWsVersion", 2);
  assert.equal(controller.get("dataAvailable"), true, "With vertex & dag but amWsVersion=2");

  vertex.set("am", {});
  assert.equal(controller.get("dataAvailable"), false, "am loaded without event time data");

  vertex.set("am", {
    initTime: Date.now()
  });
  assert.equal(controller.get("dataAvailable"), true, "am loaded with event time data");
});
