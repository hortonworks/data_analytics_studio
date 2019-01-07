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
import AutoCounterColumnMixin from '../../../mixins/auto-counter-column';
import { module, test } from 'qunit';

module('Unit | Mixin | auto counter column');

test('Basic creation test', function(assert) {
  let AutoCounterColumnObject = Ember.Object.extend(AutoCounterColumnMixin);
  let subject = AutoCounterColumnObject.create();

  assert.ok(subject);
  assert.ok(subject.columnSelectorMessage);
  assert.ok(subject.getCounterColumns);
});

test('getCounterColumns test', function(assert) {
  let TestParent = Ember.Object.extend({
    getCounterColumns: function () { return []; }
  });

  let AutoCounterColumnObject = TestParent.extend(AutoCounterColumnMixin);
  let subject = AutoCounterColumnObject.create({
    model: [{
      counterGroupsHash: {
        gp1: {
          c11: "v11",
          c12: "v12"
        }
      }
    }, {
      counterGroupsHash: {
        gp2: {
          c21: "v21",
          c22: "v22"
        },
        gp3: {
          c31: "v31",
          c32: "v32"
        }
      }
    }]
  });

  let columns = subject.getCounterColumns();
  assert.equal(columns.length, 6);
  assert.equal(columns[0].counterGroupName, "gp1");
  assert.equal(columns[0].counterName, "c11");
  assert.equal(columns[1].counterGroupName, "gp1");
  assert.equal(columns[1].counterName, "c12");

  assert.equal(columns[2].counterGroupName, "gp2");
  assert.equal(columns[2].counterName, "c21");
  assert.equal(columns[3].counterGroupName, "gp2");
  assert.equal(columns[3].counterName, "c22");

  assert.equal(columns[4].counterGroupName, "gp3");
  assert.equal(columns[4].counterName, "c31");
  assert.equal(columns[5].counterGroupName, "gp3");
  assert.equal(columns[5].counterName, "c32");
});
