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

import environment from '../../../config/environment';

moduleFor('service:env', 'Unit | Service | env', {
  // Specify the other units that are required for this test.
  // needs: ['service:foo']
});

test('Basic creation test', function(assert) {
  let service = this.subject();

  assert.ok(service);
  assert.ok(service.ENV);
  assert.ok(service.collateConfigs);
  assert.ok(service.app);
  assert.ok(service.setComputedENVs);
});

test('collateConfigs test', function(assert) {
  let service = this.subject(),
      APP = environment.APP;

  APP.a = 11;
  APP.b = 22;
  window.ENV = {
    a: 1
  };

  service.collateConfigs();

  APP = service.get("app");
  assert.equal(APP.a, 1, "Test window.ENV merge onto environment.APP");
  assert.equal(APP.b, 22);
});

test('app computed property test', function(assert) {
  let service = this.subject(),
      ENV = {
        b: 2
      };

  window.ENV = ENV;
  environment.APP.a = 11;
  service.collateConfigs();
  assert.equal(service.get("app.a"), environment.APP.a);
  assert.equal(service.get("app.b"), ENV.b);
});

test('setComputedENVs test', function(assert) {
  let service = this.subject();

  assert.equal(service.ENV.isIE, false);
});

test('Validate config/default-app-conf.js', function(assert) {
  let service = this.subject();

  assert.equal(service.get("app.hosts.timeline"), "localhost:8188");
  assert.equal(service.get("app.namespaces.webService.timeline"), "ws/v1/timeline");
  assert.equal(service.get("app.paths.timeline.dag"), "TEZ_DAG_ID");
});
