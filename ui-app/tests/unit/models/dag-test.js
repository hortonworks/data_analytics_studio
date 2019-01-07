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

moduleForModel('dag', 'Unit | Model | dag', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject(),
      testQueue = "TQ";

  Ember.run(function () {
    model.set("app", {
      queue: testQueue
    });

    assert.ok(!!model);
    assert.ok(!!model.needs.am);
    assert.ok(!!model.needs.info);
    assert.equal(model.get("queue"), testQueue);
  });

  assert.ok(model.name);
  assert.ok(model.submitter);

  assert.ok(model.vertices);
  assert.ok(model.edges);
  assert.ok(model.vertexGroups);

  assert.ok(model.domain);
  assert.ok(model.containerLogs);

  assert.ok(model.vertexIdNameMap);
  assert.ok(model.vertexNameIdMap);

  assert.ok(model.callerID);
  assert.ok(model.callerContext);
  assert.ok(model.callerDescription);
  assert.ok(model.callerType);

  assert.ok(model.dagPlan);
  assert.ok(model.callerData);
  assert.ok(model.info);

  assert.ok(model.amWsVersion);
});

test('app loadType test', function(assert) {
  let loadType = this.subject().get("needs.app.loadType"),
      record = Ember.Object.create();

  assert.equal(loadType(record), undefined);

  record.set("queueName", "Q");
  assert.equal(loadType(record), "demand");

  record.set("atsStatus", "RUNNING");
  assert.equal(loadType(record), undefined);

  record.set("atsStatus", "SUCCEEDED");
  assert.equal(loadType(record), "demand");

  record.set("queueName", undefined);
  assert.equal(loadType(record), undefined);
});

test('queue test', function(assert) {
  let model = this.subject(),
      queueName = "queueName",
      appQueueName = "AppQueueName";

  assert.equal(model.get("queue"), undefined);

  Ember.run(function () {
    model.set("app", {
      queue: appQueueName
    });
    assert.equal(model.get("queue"), appQueueName);

    model.set("queueName", queueName);
    assert.equal(model.get("queue"), queueName);
  });
});

test('vertices, edges & vertexGroups test', function(assert) {
  let testVertices = {},
      testEdges = {},
      testVertexGroups = {},
      model = this.subject({
        dagPlan: {
          vertices: testVertices,
          edges: testEdges,
          vertexGroups: testVertexGroups
        }
      });

  assert.equal(model.get("vertices"), testVertices);
  assert.equal(model.get("edges"), testEdges);
  assert.equal(model.get("vertexGroups"), testVertexGroups);

  Ember.run(function () {
    testVertices = {};
    testEdges = {};
    testVertexGroups = {};

    model.set("info", {
      dagPlan: {
        vertices: testVertices,
        edges: testEdges,
        vertexGroups: testVertexGroups
      }
    });
    assert.notEqual(model.get("vertices"), testVertices);
    assert.notEqual(model.get("edges"), testEdges);
    assert.notEqual(model.get("vertexGroups"), testVertexGroups);

    model.set("dagPlan", null);
    assert.equal(model.get("vertices"), testVertices);
    assert.equal(model.get("edges"), testEdges);
    assert.equal(model.get("vertexGroups"), testVertexGroups);
  });
});