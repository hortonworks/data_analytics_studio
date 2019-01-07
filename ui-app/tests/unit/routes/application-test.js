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

moduleFor('route:application', 'Unit | Route | application', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject({
    ldapAuth: {
      on: Ember.K
    }
  });

  assert.ok(route);
  assert.ok(route.pageReset);
  assert.ok(route.actions.didTransition);
  assert.ok(route.actions.bubbleBreadcrumbs);

  assert.ok(route.actions.error);

  assert.ok(route.actions.openModal);
  assert.ok(route.actions.closeModal);
  assert.ok(route.actions.destroyModal);

  assert.ok(route.actions.resetTooltip);
});

test('Test didTransition action', function(assert) {
  let route = this.subject({
    ldapAuth: {
      on: Ember.K
    }
  });

  assert.expect(1);

  route.pageReset = function () {
    assert.ok(true);
  };

  route.send("didTransition");
});

test('Test bubbleBreadcrumbs action', function(assert) {
  let route = this.subject({
        ldapAuth: {
          on: Ember.K
        }
      }),
      testController = {
        breadcrumbs: null
      },
      testBreadcrumbs = [{}];

  route.controller = testController;

  assert.notOk(route.get("controller.breadcrumbs"));
  route.send("bubbleBreadcrumbs", testBreadcrumbs);
  assert.equal(route.get("controller.breadcrumbs"), testBreadcrumbs);
});
