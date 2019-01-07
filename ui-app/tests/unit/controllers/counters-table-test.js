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

moduleFor('controller:counters-table', 'Unit | Controller | counters table', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  });

  assert.ok(controller);
  assert.ok(controller.columns);
  assert.ok(controller.counters);
  assert.ok(controller._countersObserver);

});

test('counters & _countersObserver test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K,
    model: {
      counterGroupsHash: {
        "foo": {
          "Foo Name 1": "Value 1",
          "Foo Name 2": "Value 2",
          "Foo Name 3": "Value 3"
        },
        "bar": {
          "Bar Name 1": "Value 1",
          "Bar Name 2": "Value 2",
          "Bar Name 3": "Value 3"
        }
      }
    }
  });

  assert.equal(controller.countersCount, 0);

  controller._countersObserver();

  assert.equal(controller.get("counters.0.groupName"), "foo");
  assert.equal(controller.get("counters.0.counterName"), "Foo Name 1");
  assert.equal(controller.get("counters.0.counterValue"), "Value 1");

  assert.equal(controller.get("counters.1.groupName"), "foo");
  assert.equal(controller.get("counters.1.counterName"), "Foo Name 2");
  assert.equal(controller.get("counters.1.counterValue"), "Value 2");

  assert.equal(controller.get("counters.2.groupName"), "foo");
  assert.equal(controller.get("counters.2.counterName"), "Foo Name 3");
  assert.equal(controller.get("counters.2.counterValue"), "Value 3");


  assert.equal(controller.get("counters.3.groupName"), "bar");
  assert.equal(controller.get("counters.3.counterName"), "Bar Name 1");
  assert.equal(controller.get("counters.3.counterValue"), "Value 1");

  assert.equal(controller.get("counters.4.groupName"), "bar");
  assert.equal(controller.get("counters.4.counterName"), "Bar Name 2");
  assert.equal(controller.get("counters.4.counterValue"), "Value 2");

  assert.equal(controller.get("counters.5.groupName"), "bar");
  assert.equal(controller.get("counters.5.counterName"), "Bar Name 3");
  assert.equal(controller.get("counters.5.counterValue"), "Value 3");

  assert.equal(controller.countersCount, 6);
});
