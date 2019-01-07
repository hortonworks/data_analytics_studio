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

moduleForComponent('error-bar', 'Integration | Component | error bar', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{error-bar}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#error-bar}}
      template block text
    {{/error-bar}}
  `);

  assert.equal(this.$().text().trim(), '');
});

test('Plain Object test', function(assert) {
  Function.prototype.bind = function () {};

  this.set("error", {});
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$().text().trim(), 'Error');
  });
});

test('Message test', function(assert) {
  var testMessage = "Test Message";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$().text().trim(), testMessage);
  });
});

test('details test', function(assert) {
  var testMessage = "Test Message",
      testDetails = "details";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage,
    details: testDetails
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$(".message").text().trim(), testMessage);
    assert.equal(this.$(".details p").text().trim(), testDetails);
  });
});

test('requestInfo test', function(assert) {
  var testMessage = "Test Message",
      testInfo = "info";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage,
    requestInfo: testInfo
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$(".message").text().trim(), testMessage);
    assert.equal(this.$(".details p").text().trim(), testInfo);
  });
});

test('stack test', function(assert) {
  var testMessage = "Test Message",
      testStack = "stack";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage,
    stack: testStack
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$(".message").text().trim(), testMessage);
    assert.equal(this.$(".details p").text().trim(), testStack);
  });
});
