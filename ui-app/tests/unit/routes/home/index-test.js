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

moduleFor('route:home/index', 'Unit | Route | home/index', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.title);

  assert.ok(route.queryParams);
  assert.ok(route.loaderQueryParams);
  assert.ok(route.setupController);

  assert.equal(route.entityType, "dag");
  assert.equal(route.loaderNamespace, "dags");

  assert.ok(route.filterRecords);

  assert.ok(route.actions.willTransition);
  assert.ok(route.actions.loadCounters);
  assert.ok(route.actions.tableRowsChanged);
});

test('refresh test', function(assert) {
  let route = this.subject();

  assert.equal(route.get("queryParams.dagName.refreshModel"), true);
  assert.equal(route.get("queryParams.dagID.refreshModel"), true);
  assert.equal(route.get("queryParams.submitter.refreshModel"), true);
  assert.equal(route.get("queryParams.status.refreshModel"), true);
  assert.equal(route.get("queryParams.appID.refreshModel"), true);
  assert.equal(route.get("queryParams.callerID.refreshModel"), true);
  assert.equal(route.get("queryParams.rowCount.refreshModel"), true);
});

test('loaderQueryParams test', function(assert) {
  let route = this.subject();
  assert.equal(Object.keys(route.get("loaderQueryParams")).length, 8);
});

test('filterRecords test', function(assert) {
  let route = this.subject(),
      testRecords = [Ember.Object.create({
        name: "test"
      }), Ember.Object.create({
        // No name
      }),Ember.Object.create({
        name: "notest"
      })],
      testQuery = {
        dagName: "test"
      };

  let filteredRecords = route.filterRecords(testRecords, testQuery);

  assert.equal(filteredRecords.length, 1);
  assert.equal(filteredRecords[0], testRecords[0]);
});

test('load - query + filter test', function(assert) {
  let testEntityID1 = "entity_1",
      testEntityID2 = "entity_2",
      testEntityID3 = "entity_3",
      testSubmitter = "testSub",

      query = {
        limit: 5,
        submitter: testSubmitter
      },
      resultRecords = Ember.A([
        Ember.Object.create({
          submitter: testSubmitter,
          entityID: testEntityID1
        }),
        Ember.Object.create(),
        Ember.Object.create(),
        Ember.Object.create(),
        Ember.Object.create({
          submitter: testSubmitter,
          entityID: testEntityID2,
          status: "RUNNING"
        }),
        Ember.Object.create({
          submitter: testSubmitter,
          entityID: testEntityID3,
        })
      ]),

      route = this.subject({
        controller: Ember.Object.create()
      });

  route.loader = Ember.Object.create({
    query: function (type, query, options) {
      assert.equal(type, "dag");
      assert.equal(query.limit, 6);
      assert.equal(options.reload, true);
      return Ember.RSVP.resolve(resultRecords);
    },
    loadNeed: function (record, field, options) {
      assert.equal(record.get("entityID"), testEntityID2);
      assert.equal(field, "am");
      assert.equal(options.reload, true);
      return Ember.RSVP.resolve();
    }
  });

  assert.expect(3 + 3 + 2 + 1 + 3 + 2);

  assert.notOk(route.get("controller.moreAvailable"), "moreAvailable shouldn't be set!");
  assert.equal(route.get("fromId"), null, "fromId shouldn't be set");

  return route.load(null, query).then(function (records) {
    assert.ok(Array.isArray(records));

    assert.equal(records.get("length"), 2, "Length should be 2!");
    assert.equal(records.get("0.entityID"), testEntityID1);
    assert.equal(records.get("1.entityID"), testEntityID2);

    assert.equal(route.get("controller.moreAvailable"), true, "moreAvailable was not set");
    assert.equal(route.get("fromId"), testEntityID3);
  });
});

test('actions.willTransition test', function(assert) {
  let route = this.subject({
    controller: Ember.Object.create()
  });

  route.set("loader", {
    unloadAll: function (type) {
      if(type === "dag" || type === "ahs-app") {
        assert.ok(true);
      }
      else {
        throw(new Error("Invalid type!"));
      }
    }
  });

  assert.expect(2);
  route.send("willTransition");
});

test('actions.loadCounters test', function(assert) {
  let route = this.subject({
        controller: Ember.Object.create()
      }),
      visibleRecords = [{}, {}, {}],
      index = 0;

  route.loader = {
    loadNeed: function (record, name) {
      assert.equal(record, visibleRecords[index++]);
      assert.equal(name, "info");
      return Ember.RSVP.resolve(record);
    }
  };
  assert.expect(3 * 2);

  route.send("loadCounters");

  route.set("visibleRecords", visibleRecords);
  route.send("loadCounters");
});