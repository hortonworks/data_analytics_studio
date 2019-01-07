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

moduleForComponent('em-tooltip', 'Integration | Component | em tooltip', {
  integration: true
});

test('Basic creation test', function(assert) {

  this.render(hbs`{{em-tooltip}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-tooltip}}
      template block text
    {{/em-tooltip}}
  `);

  assert.equal(this.$().text().trim(), '');
});

test('Title test', function(assert) {
  this.set("title", "TestTitle");
  this.render(hbs`{{em-tooltip title=title}}`);

  assert.equal(this.$().text().trim(), 'TestTitle');
});

test('Description test', function(assert) {
  this.set("desc", "TestDesc");
  this.render(hbs`{{em-tooltip description=desc}}`);

  assert.equal(this.$().text().trim(), 'TestDesc');
});

test('Properties test', function(assert) {
  this.set("properties", [{
    name: "p1", value: "v1"
  }, {
    name: "p2", value: "v2"
  }]);
  this.render(hbs`{{em-tooltip properties=properties}}`);

  assert.equal(this.$("tr").length, 2);
});

test('Contents test', function(assert) {
  this.set("contents", [{
    title: "p1",
    properties: [{}, {}]
  }, {
    title: "p2",
    properties: [{}, {}, {}]
  }]);

  this.render(hbs`{{em-tooltip contents=contents}}`);

  assert.equal(this.$(".bubble").length, 2);
  assert.equal(this.$("tr").length, 2 + 3);
});
