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

moduleFor('adapter:loader', 'Unit | Adapter | loader', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:foo']
});

test('Basic creation', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.ok(adapter._isLoader);
  assert.ok(adapter.buildURL);
  assert.ok(adapter._loaderAjax);
  assert.ok(adapter.queryRecord);
  assert.ok(adapter.query);

  assert.equal(adapter.get("name"), "loader");
});

test('buildURL test', function(assert) {
  let adapter = this.subject();

  assert.equal(adapter.buildURL("dag"), "/dags");
  assert.equal(adapter.buildURL("dag", "dag1"), "/dags/dag1");
  assert.equal(adapter.buildURL("{x}dag", "dag1", null, null, null, {x: "x_x"}), "/x_xdags/dag1", "Test for substitution");
});

test('_loaderAjax test', function(assert) {
  let adapter = this.subject(),
      testURL = "/dags",
      testQueryParams = { x:1 },
      testRecord = {},
      testNameSpace = "ns";

  assert.expect(2 + 1 + 2);

  adapter.ajax = function (url, method/*, options*/) {

    assert.equal(url, testURL);
    assert.equal(method, "GET");

    return Ember.RSVP.resolve(testRecord);
  };

  adapter.sortQueryParams = function (queryParams) {
    assert.ok(queryParams, "sortQueryParams was called with query params");
  };

  adapter._loaderAjax(testURL, testQueryParams, testNameSpace).then(function (data) {
    assert.equal(data.nameSpace, testNameSpace, "Namespace returned");
    assert.equal(data.data, testRecord, "Test record returned");
  });
});

test('queryRecord test', function(assert) {
  let adapter = this.subject(),
      testURL = "/dags",
      testModel = { modelName: "testModel" },
      testStore = {},
      testQuery = {
        id: "test1",
        params: {},
        urlParams: {},
        nameSpace: "ns"
      };

  assert.expect(4 + 3);

  adapter.buildURL = function (modelName, id, snapshot, requestType, query, params) {
    assert.equal(modelName, testModel.modelName);
    assert.equal(id, testQuery.id);
    assert.equal(query, testQuery.params);
    assert.equal(params, testQuery.urlParams);

    return testURL;
  };

  adapter._loaderAjax = function (url, queryParams, nameSpace) {
    assert.equal(url, testURL);
    assert.equal(queryParams, testQuery.params);
    assert.equal(nameSpace, testQuery.nameSpace);
  };

  adapter.queryRecord(testStore, testModel, testQuery);
});

test('query test', function(assert) {
  let adapter = this.subject(),
      testURL = "/dags",
      testModel = { modelName: "testModel" },
      testStore = {},
      testQuery = {
        id: "test1",
        params: {},
        urlParams: {},
        nameSpace: "ns"
      };

  assert.expect(5 + 3);

  adapter.buildURL = function (modelName, id, snapshot, requestType, query, params) {
    assert.equal(modelName, testModel.modelName);
    assert.equal(id, null);
    assert.equal(requestType, "query");
    assert.equal(query, testQuery.params);
    assert.equal(params, testQuery.urlParams);

    return testURL;
  };

  adapter._loaderAjax = function (url, queryParams, nameSpace) {
    assert.equal(url, testURL);
    assert.equal(queryParams, testQuery.params);
    assert.equal(nameSpace, testQuery.nameSpace);
  };

  adapter.query(testStore, testModel, testQuery);
});
