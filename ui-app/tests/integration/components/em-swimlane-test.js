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

import Process from 'hivestudio/utils/process';

moduleForComponent('em-swimlane', 'Integration | Component | em swimlane', {
  integration: true
});

test('Basic creation test', function(assert) {
  var testName1 = "TestName1",
      testName2 = "TestName2";

  this.set("processes", [Process.create({
    name: testName1
  }), Process.create({
    name: testName2
  })]);

  this.render(hbs`{{em-swimlane processes=processes}}`);

  assert.equal(this.$().text().trim().indexOf(testName1), 0);
  assert.notEqual(this.$().text().trim().indexOf(testName2), -1);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane processes=processes}}
      template block text
    {{/em-swimlane}}
  `);

  assert.equal(this.$().text().trim().indexOf(testName1), 0);
  assert.notEqual(this.$().text().trim().indexOf(testName2), -1);
});

test('Normalization (Blocker based sorting) test - On a graph', function(assert) {
  var p1 = Process.create({
    name: "P1"
  }),
  p2 = Process.create({
    name: "P2"
  }),
  p3 = Process.create({
    name: "P3",
    blockers: [p1, p2]
  }),
  p4 = Process.create({
    name: "P4",
    blockers: [p1]
  }),
  p5 = Process.create({
    name: "P5",
    blockers: [p3, p4]
  });

  this.set("processes", [p5, p4, p3, p2, p1]);

  this.render(hbs`{{em-swimlane processes=processes}}`);

  let names = this.$(".em-swimlane-process-name");

  assert.equal(names.length, 5);
  assert.equal(names.eq(0).text().trim(), p1.name);
  assert.equal(names.eq(1).text().trim(), p4.name);
  assert.equal(names.eq(2).text().trim(), p2.name);
  assert.equal(names.eq(3).text().trim(), p3.name);
  assert.equal(names.eq(4).text().trim(), p5.name);
});

test('Zoom test', function(assert) {
  this.set("processes", [Process.create()]);

  this.render(hbs`{{em-swimlane processes=processes zoom=500}}`);
  assert.equal(this.$(".zoom-panel").attr("style").trim(), "width: 500%;");
});
