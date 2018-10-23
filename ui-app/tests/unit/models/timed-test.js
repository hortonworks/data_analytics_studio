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

moduleForModel('timed', 'Unit | Model | timed', {
  // Specify the other units that are required for this test.
  needs: []
});

test('it exists', function(assert) {
  let model = this.subject();

  assert.ok(model);
  assert.ok(model.startTime);
  assert.ok(model.duration);
  assert.ok(model.endTime);
});

test('duration test', function(assert) {
  let model = this.subject();

  function resetAndCheckModel () {
    model.set("startTime", 100);
    model.set("endTime", 200);

    assert.equal(model.get("duration"), 100);
  }

  Ember.run(function () {
    resetAndCheckModel();
    model.set("endTime", 100);
    assert.equal(model.get("duration"), 0);

    model.set("startTime", 0);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("endTime", 0);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("endTime", 50);
    assert.equal(model.get("duration").message, "Start time is greater than end time by 50 msecs!");

    resetAndCheckModel();
    model.set("startTime", -100);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("endTime", -200);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("startTime", undefined);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("endTime", undefined);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("startTime", null);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("endTime", null);
    assert.equal(model.get("duration"), undefined);
  });
});
