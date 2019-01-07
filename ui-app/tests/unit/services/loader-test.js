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

moduleFor('service:loader', 'Unit | Service | loader', {
  // Specify the other units that are required for this test.
  // needs: ['service:foo']
});

test('Basic creation test', function(assert) {
  let service = this.subject();

  assert.ok(service.cache);
  assert.ok(service.store);
  assert.ok(service._setOptions);

  assert.ok(service.checkRequisite);

  assert.ok(service.lookup);
  assert.ok(service.entityFor);

  assert.ok(service.getCacheKey);

  assert.ok(service.queryRecord);
  assert.ok(service.query);

  assert.ok(service.unloadAll);
});

test('_setOptions test', function(assert) {
  let service = this.subject();

  assert.equal(service.get("nameSpace"), '');

  service._setOptions({
    nameSpace: "ns"
  });

  assert.equal(service.get("nameSpace"), 'ns');
});

test('checkRequisite test', function(assert) {
  let service = this.subject(),
      testType = "type";

  assert.expect(3 + 3 + 2);

  // Not found
  service.store = {
    adapterFor: function (type) {
      assert.equal(type, testType);
    },
    serializerFor: function (type) {
      assert.equal(type, testType);
    }
  };
  assert.throws(function () {
    service.checkRequisite(testType);
  });

  // Not loader found
  service.store = {
    adapterFor: function (type) {
      assert.equal(type, testType);
      return {};
    },
    serializerFor: function (type) {
      assert.equal(type, testType);
      return {};
    }
  };
  assert.throws(function () {
    service.checkRequisite(testType);
  });

  service.store = {
    adapterFor: function (type) {
      assert.equal(type, testType);
      return { _isLoader: true };
    },
    serializerFor: function (type) {
      assert.equal(type, testType);
      return { _isLoader: true };
    }
  };

  service.checkRequisite(testType);
});

test('lookup test', function(assert) {
  let service = this.subject();

  assert.expect(1);

  service.container.lookup = function (fullName) {
    assert.equal(fullName, "typ:na-me");
  };

  service.lookup("typ", "NaMe");
});

test('entityFor test', function(assert) {
  let service = this.subject(),
      testName = "abc",
      entity;

  assert.expect(3 + 4 + 3);

  // All lookups fail
  service.lookup = function (type, name) {
    if(name === testName) {
      assert.equal(type, "entitie");
    }
    if(name === "entity") {
      assert.equal(type, "entitie");
    }
  };
  assert.throws(function () {
    service.entityFor(testName);
  }, "All lookups fail");

  // Default lookups succeeded
  service.lookup = function (type, name) {
    if(name === testName) {
      assert.equal(type, "entitie");
    }
    if(name === "entity") {
      assert.equal(type, "entitie");
      return Ember.Object.create({
        actualName: "entity",
        name: name
      });
    }
  };
  entity = service.entityFor(testName);
  assert.equal(entity.actualName, "entity", "Default lookups succeeded");
  assert.equal(entity.get("name"), testName, "Default lookups succeeded");

  // Primary lookups succeeded
  service.lookup = function (type, name) {
    if(name === testName) {
      assert.equal(type, "entitie");
      return Ember.Object.create({
        actualName: name,
        name: name
      });
    }
    if(name === "entity") {
      assert.equal(type, "entitie"); // Shouldn't be called
    }
  };
  entity = service.entityFor(testName);
  assert.equal(entity.get("name"), testName, "Default lookups succeeded");
  assert.equal(entity.get("name"), testName, "Default lookups succeeded");
});

test('getCacheKey test', function(assert) {
  let service = this.subject();

  assert.equal(service.getCacheKey("type"), "type");
  assert.equal(service.getCacheKey("type", {a:1}), 'type:{"a":1}');
  assert.equal(service.getCacheKey("type", null, 1), "type:1");
  assert.equal(service.getCacheKey("type", {a:1}, 1), 'type:1:{"a":1}');
});

test('queryRecord test', function(assert) {
  let service = this.subject(),
      testNameSpace = "ns",
      testOptions = {opt: 1},
      testQueryParams = {},
      testUrlParams = {},
      testType = "type",
      testRecord = {},
      testID = 1,
      cacheKey = service.getCacheKey(testType, testQueryParams, testID);

  assert.expect(1 + 5 + 3);

  service.nameSpace = testNameSpace;
  service.checkRequisite = Ember.K;
  service.entityFor = function (type) {
    assert.equal(type, testType);

    return {
      queryRecord: function (loader, id, options, query, urlParams) {
        assert.equal(loader, service, "Loader");
        assert.equal(id, testID, "id");
        assert.equal(options.opt, testOptions.opt, "options");
        assert.equal(query, testQueryParams, "query");
        assert.equal(urlParams, testUrlParams, "urlParams");

        return Ember.RSVP.resolve(testRecord);
      }
    };
  };

  service.cache = Ember.Object.create();
  assert.notOk(service.get("cache").get(cacheKey));
  service.queryRecord(testType, testID, testOptions, testQueryParams, testUrlParams).then(function (record) {
    assert.equal(record, testRecord);
  });
  assert.ok(service.get("cache").get(cacheKey));
});

test('query test', function(assert) {
  let service = this.subject(),
      testNameSpace = "ns",
      testOptions = {opt: 1},
      testQueryParams = {},
      testUrlParams = {},
      testType = "type",
      testRecord = {},
      testRecords = [testRecord, testRecord],
      cacheKey = service.getCacheKey(testType, testQueryParams);

  assert.expect(1 + 4 + 3);

  service.nameSpace = testNameSpace;
  service.checkRequisite = Ember.K;
  service.entityFor = function (type) {
    assert.equal(type, testType);

    return {
      query: function (loader, query, options, urlParams) {
        assert.equal(loader, service, "Loader");
        assert.equal(options.opt, testOptions.opt, "options");
        assert.equal(query, testQueryParams, "query");
        assert.equal(urlParams, testUrlParams, "urlParams");

        return Ember.RSVP.resolve(testRecords);
      }
    };
  };

  service.cache = Ember.Object.create();
  assert.notOk(service.get("cache").get(cacheKey));
  service.query(testType, testQueryParams, testOptions, testUrlParams).then(function (records) {
    assert.equal(records, testRecords);
  });
  assert.ok(service.get("cache").get(cacheKey));
});

test('unloadAll test', function(assert) {
  let testType1 = "a",
      service = this.subject({
        nameSpace: "ns",
        store: {
          peekAll: function (type) {
            assert.equal(type, testType1);
            return [Ember.Object.create({
              id: "ns:id1",
              entityID: "id1"
            }), Ember.Object.create({
              id: "nsX:id1",
              entityID: "id1"
            })];
          },
          unloadRecord: function (record) {
            assert.equal(record.get("entityID"), "id1");
          }
        }
      });

  assert.expect(1 + 1);

  service.unloadAll(testType1, "id2");
});

test('unloadAll skipID test', function(assert) {
  let testType1 = "q",
      service = this.subject({
        nameSpace: "ns",
        store: {
          peekAll: function (type) {
            assert.equal(type, testType1);
            return [Ember.Object.create({
              id: "ns:id1",
              entityID: "id1"
            }), Ember.Object.create({
              id: "ns:id2",
              entityID: "id2"
            })];
          },
          unloadRecord: function (record) {
            assert.equal(record.get("entityID"), "id2");
          }
        }
      });

  assert.expect(1 + 1);

  service.unloadAll(testType1, "id1");
});
