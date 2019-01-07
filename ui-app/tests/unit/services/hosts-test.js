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

import { moduleFor, test } from 'ember-qunit';

moduleFor('service:hosts', 'Unit | Service | hosts', {
  // Specify the other units that are required for this test.
  needs: ['service:env']
});

test('Test creation', function(assert) {
  let service = this.subject();
  assert.ok(service);
});

test('Test correctProtocol', function(assert) {
  let service = this.subject();

  //No correction
  assert.equal(service.correctProtocol("http://localhost:8088"), "http://localhost:8088");

  // Correction
  assert.equal(service.correctProtocol("localhost:8088"), "http://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088"), "http://localhost:8088");
  assert.equal(service.correctProtocol("file://localhost:8088"), "http://localhost:8088");

  assert.equal(service.correctProtocol("localhost:8088", "http:"), "http://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088", "http:"), "http://localhost:8088");
  assert.equal(service.correctProtocol("file://localhost:8088", "http:"), "http://localhost:8088");

  assert.equal(service.correctProtocol("localhost:8088", "https:"), "https://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088", "https:"), "https://localhost:8088");
  assert.equal(service.correctProtocol("file://localhost:8088", "https:"), "https://localhost:8088");
});

test('Test correctProtocol with protocol=file:', function(assert) {
  let service = this.subject();

  assert.equal(service.correctProtocol("file://localhost:8088", "file:"), "file://localhost:8088");
  assert.equal(service.correctProtocol("http://localhost:8088", "file:"), "http://localhost:8088");
  assert.equal(service.correctProtocol("https://localhost:8088", "file:"), "https://localhost:8088");
});

test('Test host URLs', function(assert) {
  let service = this.subject();

  assert.equal(service.get("timeline"), "http://localhost:8188");
  assert.equal(service.get("rm"), "http://localhost:8088");
});

test('Test host URLs with ENV set', function(assert) {
  let service = this.subject();

  window.ENV = {
    hosts: {
      timeline: "https://localhost:3333",
      rm: "https://localhost:4444"
    }
  };
  assert.equal(service.get("timeline"), "http://localhost:3333");
  assert.equal(service.get("rm"), "http://localhost:4444");
});
