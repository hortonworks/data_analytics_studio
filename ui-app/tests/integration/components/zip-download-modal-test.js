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

moduleForComponent('zip-download-modal', 'Integration | Component | zip download modal', {
  integration: true
});

test('Basic creation test', function(assert) {
  var testID = "dag_a",
      expectedMessage = "Downloading data for dag: " + testID;

  this.set("content", {
    dag: {
      entityID: testID
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#zip-download-modal content=content}}
      template block text
    {{/zip-download-modal}}
  `);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);
});

test('progress test', function(assert) {
  this.set("content", {
    downloader: {
      percent: 0.5
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  let text = this.$(".message").text().trim();
  assert.equal(text.substr(-3), "50%");

  assert.equal(this.$(".btn").length, 1);
  assert.equal(this.$(".btn-primary").length, 0);
});

test('failed test', function(assert) {
  var expectedMessage = "Error downloading data!";

  this.set("content", {
    downloader: {
      failed: true
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);

  assert.equal(this.$(".btn").length, 1);
  assert.equal(this.$(".btn-primary").length, 1);
});

test('partial test', function(assert) {
  var expectedMessage = "Data downloaded might be incomplete. Please check the zip!";

  this.set("content", {
    downloader: {
      succeeded: true,
      partial: true
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);

  assert.equal(this.$(".btn").length, 1);
  assert.equal(this.$(".btn-primary").length, 1);
});