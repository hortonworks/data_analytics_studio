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

import wait from 'ember-test-helpers/wait';

import Process from 'hivestudio/utils/process';

moduleForComponent('em-swimlane-vertex-name', 'Integration | Component | em swimlane vertex name', {
  integration: true
});

test('Basic creation test', function(assert) {

  this.render(hbs`{{em-swimlane-vertex-name}}`);
  assert.equal(this.$().text().trim(), 'Not Available!');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane-vertex-name}}
      template block text
    {{/em-swimlane-vertex-name}}
  `);
  assert.equal(this.$().text().trim(), 'Not Available!');
});

test('Name test', function(assert) {
  this.set("process", Process.create({
    name: "TestName"
  }));

  this.render(hbs`{{em-swimlane-vertex-name process=process}}`);
  return wait().then(() => {
    var content = this.$().text().trim();
    assert.equal(content.substr(content.length - 8), 'TestName');
  });
});

test('Progress test', function(assert) {
  this.set("process", Process.create({
    vertex: {
      finalStatus: "RUNNING",
      progress: 0.5
    }
  }));

  this.render(hbs`{{em-swimlane-vertex-name process=process}}`);
  return wait().then(() => {
    assert.equal(this.$(".progress-text").text().trim(), '50%');
  });
});

test('finalStatus test', function(assert) {
  this.set("process", Process.create({
    vertex: {
      finalStatus: "STAT"
    }
  }));

  this.render(hbs`{{em-swimlane-vertex-name process=process}}`);
  return wait().then(() => {
    assert.equal(this.$().text().trim(), 'STAT');
  });
});
