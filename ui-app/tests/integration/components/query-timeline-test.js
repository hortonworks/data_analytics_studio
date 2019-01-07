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

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('query-timeline', 'Integration | Component | query timeline', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("perf", {});
  this.render(hbs`{{query-timeline perf=perf}}`);

  assert.equal(this.$().find(".bar").length, 9 + 4);

  this.set("perf", null);
  this.render(hbs`{{query-timeline perf=perf}}`);

  assert.equal(this.$().find(".bar").length, 9 + 4);

  this.render(hbs`
    {{#query-timeline perf=perf}}
      template block text
    {{/query-timeline}}
  `);

  assert.equal(this.$().find(".bar").length, 9 + 4);
});

test('Default value test', function(assert) {
  this.set("perf", {});
  this.render(hbs`{{query-timeline perf=perf}}`);

  let bars = this.$().find(".sub-groups").find(".bar");
  assert.equal(bars.length, 9);

  assert.equal(bars[0].style.width, 0);
  assert.equal(bars[1].style.width, 0);
  assert.equal(bars[2].style.width, 0);
  assert.equal(bars[3].style.width, 0);
  assert.equal(bars[4].style.width, 0);
  assert.equal(bars[5].style.width, 0);
  assert.equal(bars[6].style.width, 0);
  assert.equal(bars[7].style.width, 0);
  assert.equal(bars[8].style.width, 0);
});

test('alignBars test', function(assert) {
  var total = 10 + 20 + 40 + 50 + 60 + 70 + 80 + 90 + 100;
  var bars;

  this.set("perf", {
    "compile": 10,
    "parse": 20,
    "TezBuildDag": 40,

    "TezSubmitDag": 50,
    "TezSubmitToRunningDag": 60,

    "TezRunDag": 70,

    "PostHiveProtoLoggingHook": 80,
    "RemoveTempOrDuplicateFiles": 90,
    "RenameOrMoveFiles": 100,
  });
  this.render(hbs`{{query-timeline perf=perf}}`);

  function assertWidth(domElement, factor) {
    var elementWidth = (parseFloat(domElement.style.width) / 100).toFixed(4),
        expectedWidth = (factor / total).toFixed(4);

    assert.equal(elementWidth, expectedWidth, `Unexpected value for factor ${factor}`);
  }

  bars = this.$().find(".groups").find(".bar");
  assert.equal(bars.length, 4);
  assertWidth(bars[0], 10 + 20 + 40);
  assertWidth(bars[1], 50 + 60);
  assertWidth(bars[2], 70);
  assertWidth(bars[3], 80 + 90 + 100);

  bars = this.$().find(".sub-groups").find(".bar");
  assert.equal(bars.length, 9);
  assertWidth(bars[0], 10);
  assertWidth(bars[1], 20);
  assertWidth(bars[2], 40);
  assertWidth(bars[3], 50);
  assertWidth(bars[4], 60);
  assertWidth(bars[5], 70);
  assertWidth(bars[6], 80);
  assertWidth(bars[7], 90);
  assertWidth(bars[8], 100);
});

test('alignBars - without RenameOrMoveFiles test', function(assert) {
  var total = 10 + 20 + 40 + 50 + 60 + 70 + 80 + 90 + 0;
  var bars;

  this.set("perf", {
    "compile": 10,
    "parse": 20,
    "TezBuildDag": 40,

    "TezSubmitDag": 50,
    "TezSubmitToRunningDag": 60,

    "TezRunDag": 70,

    "PostHiveProtoLoggingHook": 80,
    "RemoveTempOrDuplicateFiles": 90,
    // RenameOrMoveFiles not added
  });
  this.render(hbs`{{query-timeline perf=perf}}`);

  function assertWidth(domElement, factor) {
    var elementWidth = (parseFloat(domElement.style.width) / 100).toFixed(4),
        expectedWidth = (factor / total).toFixed(4);

    assert.equal(elementWidth, expectedWidth, `Unexpected value for factor ${factor}`);
  }

  bars = this.$().find(".groups").find(".bar");
  assert.equal(bars.length, 4);
  assertWidth(bars[0], 10 + 20 + 40);
  assertWidth(bars[1], 50 + 60);
  assertWidth(bars[2], 70);
  assertWidth(bars[3], 80 + 90);

  bars = this.$().find(".sub-groups").find(".bar");
  assert.equal(bars.length, 9);
  assertWidth(bars[0], 10);
  assertWidth(bars[1], 20);
  assertWidth(bars[2], 40);
  assertWidth(bars[3], 50);
  assertWidth(bars[4], 60);
  assertWidth(bars[5], 70);
  assertWidth(bars[6], 80);
  assertWidth(bars[7], 90);
  assertWidth(bars[8], 0);
});

test('tables test', function(assert) {
  this.set("perf", {
    "PostHiveProtoLoggingHook": 80,
    "RemoveTempOrDuplicateFiles": 90,
    "RenameOrMoveFiles": 100,
  });
  this.render(hbs`{{query-timeline perf=perf}}`);

  assert.equal(this.$().find("table").length, 4);
  assert.equal(this.$().find(".detail-list").length, 4);

  assert.equal(this.$().find("table").find("td").length, 9 * 2);
  assert.equal(this.$().find("table").find("i").length, 9);
});

test('tables post test', function(assert) {
  this.set("perf", {});
  this.render(hbs`{{query-timeline perf=perf}}`);

  assert.equal(this.$().find("table").length, 4);
  assert.equal(this.$().find(".detail-list").length, 4);

  assert.equal(this.$().find("table").find("td").length, 6 * 2);
  assert.equal(this.$().find("table").find("i").length, 6);
});
