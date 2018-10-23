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
import { moduleFor, test } from 'ember-qunit';

moduleFor('route:home/queries', 'Unit | Route | home/queries', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('it exists', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.title);

  assert.ok(route.queryParams);
  assert.ok(route.loaderQueryParams);
  assert.ok(route.setupController);

  assert.equal(route.entityType, "hive-query");
  assert.equal(route.loaderNamespace, "queries");

  assert.ok(route.actions.willTransition);
});

test('refresh test', function(assert) {
  let route = this.subject();

  assert.equal(route.get("queryParams.queryID.refreshModel"), true);
  assert.equal(route.get("queryParams.dagID.refreshModel"), true);
  assert.equal(route.get("queryParams.appID.refreshModel"), true);
  assert.equal(route.get("queryParams.executionMode.refreshModel"), true);
  assert.equal(route.get("queryParams.user.refreshModel"), true);
  assert.equal(route.get("queryParams.requestUser.refreshModel"), true);
  assert.equal(route.get("queryParams.tablesRead.refreshModel"), true);
  assert.equal(route.get("queryParams.tablesWritten.refreshModel"), true);
  assert.equal(route.get("queryParams.operationID.refreshModel"), true);
  assert.equal(route.get("queryParams.queue.refreshModel"), true);

  assert.equal(route.get("queryParams.rowCount.refreshModel"), true);
});

test('loaderQueryParams test', function(assert) {
  let route = this.subject();
  assert.equal(Object.keys(route.get("loaderQueryParams")).length, 10 + 1);
});

test('load - query test', function(assert) {
  let route = this.subject({
        controller: Ember.Object.create()
      }),
      testEntityID1 = "entity_1",
      testSubmitter = "sub",
      query = {
        limit: 5,
        submitter: testSubmitter
      },
      resultRecords = Ember.A([
        Ember.Object.create({
          submitter: testSubmitter,
          entityID: testEntityID1
        })
      ]);

  route.loader = Ember.Object.create({
    query: function (type, query, options) {
      assert.equal(type, "hive-query");
      assert.equal(query.limit, 6);
      assert.equal(options.reload, true);
      return Ember.RSVP.resolve(resultRecords);
    },
  });

  assert.expect(3 + 1 + 2);

  return route.load(null, query).then(function (records) {
    assert.ok(Array.isArray(records));

    assert.equal(records.get("length"), 1);
    assert.equal(records.get("0.entityID"), testEntityID1);
  });

});

test('actions.willTransition test', function(assert) {
  let route = this.subject({
    controller: Ember.Object.create()
  });

  route.set("loader", {
    unloadAll: function (type) {
      if(type === "hive-query") {
        assert.ok(true);
      }
      else {
        throw(new Error("Invalid type!"));
      }
    }
  });

  assert.expect(1);
  route.send("willTransition");
});
