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

moduleFor('entitie:entity', 'Unit | Entity | entity', {
  // Specify the other units that are required for this test.
  // needs: ['entitie:foo']
});

test('Basic creation test', function(assert) {
  let entity = this.subject();

  assert.ok(entity);

  assert.ok(entity.queryRecord);
  assert.ok(entity.query);

  assert.ok(entity.normalizeNeed);
  assert.ok(entity.setNeed);
  assert.ok(entity._loadNeed);
  assert.ok(entity.loadNeed);

  assert.ok(entity._loadAllNeeds);
  assert.ok(entity.loadAllNeeds);

  assert.ok(entity.resetAllNeeds);
});

test('normalizeNeed test', function(assert) {
  let entity = this.subject(),
      expectedProperties = ["id", "name", "type", "idKey", "silent", "queryParams", "urlParams"],
      testParentModel = Ember.Object.create({
        appKey: "id_1"
      }),
      testQueryParams = { x: 1 },
      testUrlParams = { y: 2 };

  assert.deepEqual(entity.normalizeNeed("app", "appKey", testParentModel, testQueryParams, testUrlParams).
  getProperties(expectedProperties), {
    id: "id_1",
    name: "app",
    type: "app",
    idKey: "appKey",
    silent: false,
    queryParams: testQueryParams,
    urlParams: testUrlParams
  }, "Test 1");

  assert.deepEqual(entity.normalizeNeed( "app", {
    idKey: "appKey",
    queryParams: { x: 3 },
    urlParams: { y: 4 }
  }, testParentModel).
  getProperties(expectedProperties), {
    id: "id_1",
    name: "app",
    type: "app",
    idKey: "appKey",
    silent: false,
    queryParams: { x: 3 },
    urlParams: { y: 4 }
  }, "Test 2");

  assert.deepEqual(entity.normalizeNeed( "app", {
    type: "application",
    idKey: "appKey",
    queryParams: { x: 3 },
    urlParams: { y: 4 }
  }, testParentModel, testQueryParams, testUrlParams).
  getProperties(expectedProperties), {
    id: "id_1",
    name: "app",
    type: "application",
    idKey: "appKey",
    silent: false,
    queryParams: testQueryParams,
    urlParams: testUrlParams
  }, "Test 3");

  assert.deepEqual(entity.normalizeNeed( "app", {
    silent: true,
    idKey: "appKey",
    queryParams: function () {
      return { x: 5 };
    },
    urlParams: function () {
      return { y: 6 };
    },
  }, testParentModel).
  getProperties(expectedProperties), {
    id: "id_1",
    name: "app",
    type: "app",
    idKey: "appKey",
    silent: true,
    queryParams: { x: 5 },
    urlParams: { y: 6}
  }, "Test 4");
});

test('loadAllNeeds basic test', function(assert) {
  let entity = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: "appID",
          foo: "fooID"
        },
        appID: 1,
        fooID: 2
      });

  assert.expect(1 + 2 + 1);

  assert.equal(entity.loadAllNeeds(loader, Ember.Object.create()), undefined, "Model without needs");

  loader = {
    queryRecord: function (type, id) {

      // Must be called twice, once for each record
      switch(type) {
        case "app":
          assert.equal(id, testModel.get("appID"));
        break;
        case "foo":
          assert.equal(id, testModel.get("fooID"));
        break;
      }

      return Ember.RSVP.resolve();
    }
  };
  entity.loadAllNeeds(loader, testModel).then(function () {
    assert.ok(true);
  });
});

test('loadAllNeeds silent=false test', function(assert) {
  let entity = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            idKey: "appID",
            // silent: false - By default it's false
          },
        },
        appID: 1,
      }),
      testErr = {};

  assert.expect(1 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      return Ember.RSVP.reject(testErr);
    }
  };
  entity.loadAllNeeds(loader, testModel).catch(function (err) {
    assert.equal(err, testErr);
  });
});

test('loadAllNeeds silent=true test', function(assert) {
  let entity = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            idKey: "appID",
            silent: true
          },
        },
        appID: 1,
      });

  assert.expect(1 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      return Ember.RSVP.resolve();
    }
  };
  entity.loadAllNeeds(loader, testModel).then(function (val) {
    assert.ok(val);
  });
});

test('setNeed test', function(assert) {
  let entity = this.subject(),
      parentModel = Ember.Object.create({
        refreshLoadTime: function () {
          assert.ok(true);
        }
      }),
      testModel = {},
      testName = "name";

  assert.expect(1 + 2);

  entity.setNeed(parentModel, testName, testModel);
  assert.equal(parentModel.get(testName), testModel);

  parentModel.set("isDeleted", true);
  parentModel.set(testName, undefined);
  entity.setNeed(parentModel, testName, testModel);
  assert.equal(parentModel.get(testName), undefined);
});

test('loadAllNeeds loadType=function test', function(assert) {
  var entity = this.subject(),
      loader = {},
      testRecord = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            idKey: "appID",
            loadType: function (record) {
              assert.ok(testRecord === record);
              return "demand";
            }
          },
        },
        appID: 1,
      });

  entity._loadNeed = function () {
    assert.ok(true); // Shouldn't be called
  };

  assert.expect(1 + 1);
  assert.equal(entity.loadAllNeeds(loader, testRecord), undefined);
});

test('_loadNeed single string type test', function(assert) {
  let entity = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            type: "appRm",
            idKey: "appID",
            silent: true
          },
        },
        appID: 1,
      });

  assert.expect(2 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      assert.equal(type, "appRm");
      return Ember.RSVP.resolve();
    }
  };
  entity.loadAllNeeds(loader, testModel).then(function (val) {
    assert.ok(val);
  });
});

test('_loadNeed multiple type test', function(assert) {
  let entity = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            type: ["AhsApp", "appRm"],
            idKey: "appID",
            silent: true
          },
        },
        appID: 1,
      });

  assert.expect(2 * 2 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));

      if(type === "AhsApp") {
        assert.ok(true);
        return Ember.RSVP.reject();
      }
      else {
        assert.equal(type, "appRm");
        return Ember.RSVP.resolve();
      }
    }
  };
  entity.loadAllNeeds(loader, testModel).then(function (val) {
    assert.ok(val);
  });
});

test('_loadNeed test with silent false', function(assert) {
  let entity = this.subject(),
      loader,
      testModel = Ember.Object.create({
        refreshLoadTime: Ember.K,
        needs: {
          app: {
            type: ["AhsApp"],
            idKey: "appID",
            silent: false
          },
        },
        appID: 1,
      }),
      testErr = {};

  assert.expect(2 + 1);

  loader = {
    queryRecord: function (type, id) {
      assert.equal(id, testModel.get("appID"));
      assert.equal(type, "AhsApp");
      return Ember.RSVP.reject(testErr);
    }
  };
  entity.loadAllNeeds(loader, testModel).catch(function (err) {
    assert.equal(err, testErr);
  });
});

test('resetAllNeeds test', function(assert) {
  let entity = this.subject(),
      parentModel = Ember.Object.create({
        needs : {
          foo: {},
          bar: {}
        },
        foo: 1,
        bar: 2,
        refreshLoadTime: function () {
          assert.ok(true);
        }
      });

  assert.expect(2 + 2 + 2);

  assert.equal(parentModel.get("foo"), 1);
  assert.equal(parentModel.get("bar"), 2);

  entity.resetAllNeeds({}, parentModel);

  assert.equal(parentModel.get("foo"), null);
  assert.equal(parentModel.get("bar"), null);
});