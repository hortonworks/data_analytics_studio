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

import hbs from 'htmlbars-inline-precompile';
import { moduleForComponent, test } from 'ember-qunit';
import wait from 'ember-test-helpers/wait';

moduleForComponent('queries-page-search', 'Integration | Component | queries page search', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{queries-page-search}}`);
  assert.equal(this.$("input").length, 8);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#queries-page-search}}
      template block text
    {{/queries-page-search}}
  `);
  assert.equal(this.$("input").length, 8);
});

test('tableDefinition test', function(assert) {
  var testQueryID = "query_1",
      testUser = "user",
      testTablesRead = "TablesRead",
      testTablesWritten = "TablesWritten",
      testAppID = "AppID",
      testDagID = "DAGID",
      testQueue = "queue",
      testExecutionMode = "ExecutionMode";

  this.set("tableDefinition", Ember.Object.create({
    queryID: testQueryID,
    requestUser: testUser,
    tablesRead: testTablesRead,
    tablesWritten: testTablesWritten,
    appID: testAppID,
    dagID: testDagID,
    queue: testQueue,
    executionMode: testExecutionMode,
  }));

  this.render(hbs`{{queries-page-search tableDefinition=tableDefinition}}`);

  return wait().then(() => {
    assert.equal(this.$('input').length, 8);
    assert.equal(this.$('input').eq(0).val(), testQueryID);
    assert.equal(this.$('input').eq(1).val(), testUser);
    assert.equal(this.$('input').eq(2).val(), testDagID);
    assert.equal(this.$('input').eq(3).val(), testTablesRead);
    assert.equal(this.$('input').eq(4).val(), testTablesWritten);
    assert.equal(this.$('input').eq(5).val(), testAppID);
    assert.equal(this.$('input').eq(6).val(), testQueue);
    assert.equal(this.$('input').eq(7).val(), testExecutionMode);
  });
});
