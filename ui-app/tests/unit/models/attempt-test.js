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

moduleForModel('attempt', 'Unit | Model | attempt', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(model);

  assert.ok(model.needs.dag);
  assert.ok(model.needs.am);

  assert.ok(model.taskID);
  assert.ok(model.taskIndex);

  assert.ok(model.vertexID);
  assert.ok(model.vertexIndex);
  assert.ok(model.vertexName);

  assert.ok(model.dagID);
  assert.ok(model.dag);

  assert.ok(model.containerID);
  assert.ok(model.nodeID);

  assert.ok(model.inProgressLogsURL);
  assert.ok(model.completedLogsURL);
  assert.ok(model.logURL);
  assert.ok(model.containerLogURL);
});

test('index test', function(assert) {
  let model = this.subject({
    entityID: "1_2_3"
  });

  assert.equal(model.get("index"), "3");
});

test('taskIndex test', function(assert) {
  let model = this.subject({
        taskID: "1_2_3",
      });

  assert.equal(model.get("taskIndex"), "3");
});

test('vertexName test', function(assert) {
  let testVertexName = "Test Vertex",
      model = this.subject({
        vertexID: "1_2",
        dag: {
          vertexIdNameMap: {
            "1_2": testVertexName
          }
        }
      });

  assert.equal(model.get("vertexName"), testVertexName);
});

test('logURL test', function(assert) {
  let model = this.subject({
        entityID: "id_1",
        dag: Ember.Object.create(),
        env: {
          app: {
            yarnProtocol: "ptcl"
          }
        },
        completedLogsURL: "http://abc.com/completed/link.log.done"
      });

  Ember.run(function () {
    // Normal Tez log link
    model.set("inProgressLogsURL", "abc.com/test/link");
    assert.equal(model.get("logURL"), "ptcl://abc.com/test/link/syslog_id_1");

    // LLAP log link - In Progress
    model.set("inProgressLogsURL", "http://abc.com/in-progress/link.log");
    assert.equal(model.get("logURL"), "http://abc.com/in-progress/link.log");

    // LLAP log link - Completed
    model.set("dag.isComplete", true);
    assert.equal(model.get("logURL"), "http://abc.com/completed/link.log.done");
  });
});