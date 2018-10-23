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

moduleFor('route:server-side-ops', 'Unit | Route | server side ops', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.load);
  assert.ok(route.loadNewPage);

  assert.ok(route.actions.loadPage);
  assert.ok(route.actions.reload);

  assert.ok(route.actions.willTransition);
});

test('load - query/filter test', function(assert) {
  let testEntityType = "EntityType",
      testEntityID1 = "entity_1",
      testEntityID2 = "entity_2",
      testFromID = "entity_6",

      query = {
        limit: 5
      },
      resultRecords = Ember.A([
        Ember.Object.create({
          entityID: testEntityID1
        }),
        {}, {}, {}, {},
        Ember.Object.create({
          entityID: testFromID
        })
      ]),

      route = this.subject({
        entityType: testEntityType,
        controller: Ember.Object.create(),
        loader: {
          query: function (type, query, options) {
            assert.equal(type, testEntityType);
            assert.equal(query.limit, 6);
            assert.equal(options.reload, true);
            return Ember.RSVP.resolve(resultRecords);
          }
        }
      });

  assert.expect(3 * 2 + 2 + 3 + 3);

  assert.notOk(route.get("controller.moreAvailable"));
  assert.equal(route.get("fromId"), null);

  return route.load(null, query).then(function (records) {
    assert.equal(records.get("0.entityID"), testEntityID1);

    assert.equal(route.get("controller.moreAvailable"), true, "moreAvailable was not set");
    assert.equal(route.get("fromId"), testFromID);
  }).then(function () {
    resultRecords = Ember.A([
      Ember.Object.create({
        entityID: testEntityID2
      })
    ]);
    return route.load(null, query);
  }).then(function (records) {
    assert.equal(records.get("0.entityID"), testEntityID2);

    assert.equal(route.get("controller.moreAvailable"), false);
    assert.equal(route.get("fromId"), null);
  });
});

test('load - id fetch test', function(assert) {
  let testEntityType = "EntityType",
      testRecord = Ember.Object.create(),
      route = this.subject({
        entityType: testEntityType,
        controller: Ember.Object.create(),
        loader: {
          queryRecord: function (type, id, options) {
            assert.equal(type, testEntityType);
            assert.equal(options.reload, true);
            if (id === querySuccess.id) {
              return Ember.RSVP.resolve(testRecord);
            } else {
              return Ember.RSVP.reject(new Error("Failed in Reject"));
            }
          }
        }
      }),
      querySuccess = {
        id :'entity_123'
      },
      queryFailure = {
        id :'entity_456'
      };

  assert.expect(2 * 2 + 3 + 1);

  route.load(null, querySuccess).then(function (records) {
    assert.ok(Array.isArray(records));
    assert.equal(records.length, 1);
    assert.equal(records[0], testRecord);
  });
  route.load(null, queryFailure).then(function (data) {
    assert.equal(data.length,0);
  });
});

test('loadNewPage test', function(assert) {
  let currentQuery = {
        val: {}
      },
      data = [],
      fromId = "id1",
      route = this.subject({
        controller: Ember.Object.create(),
        currentQuery: currentQuery,
        fromId: fromId,
        loadedValue: {
          pushObjects: function (objs) {
            assert.equal(data, objs);
          }
        },
        load: function (value, query) {
          assert.equal(query.val, currentQuery.val);
          assert.equal(query.fromId, fromId);
          return Ember.RSVP.resolve(data);
        }
      });

  assert.expect(1 + 2);

  route.loadNewPage();
});

test('actions.willTransition test', function(assert) {
  let testPageNum = 5,
      controller = Ember.Object.create({
        pageNum: testPageNum
      }),
      route = this.subject({
        controller: controller,
      });

  assert.expect(1 + 1);

  assert.equal(controller.get("pageNum"), testPageNum);
  route.send("willTransition");
  assert.equal(controller.get("pageNum"), 1); // PageNum must be reset
});
