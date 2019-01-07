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

moduleFor('adapter:timeline', 'Unit | Adapter | timeline', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:foo']
});

test('Basic creation test', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.ok(adapter.filters);
  assert.ok(adapter.stringifyFilters);
  assert.ok(adapter.normalizeQuery);
  assert.ok(adapter.query);

  assert.equal(adapter.serverName, "timeline");
});

test('filters test', function(assert) {
  let filters = this.subject().filters;
  assert.equal(Object.keys(filters).length, 6 + 8 + 4);
});

test('stringifyFilters test', function(assert) {
  let adapter = this.subject();

  assert.equal(adapter.stringifyFilters({a: 1, b: 2}), 'a:"1",b:"2"');
  assert.throws(function () {
    adapter.stringifyFilters();
  });

  assert.equal(adapter.stringifyFilters({a: "123", b: "abc"}), 'a:"123",b:"abc"');
  assert.equal(adapter.stringifyFilters({a: '123', b: 'abc'}), 'a:"123",b:"abc"');
  assert.equal(adapter.stringifyFilters({a: '123"abc'}), 'a:"123\\"abc"');
});

test('normalizeQuery test', function(assert) {
  let adapter = this.subject(),
      normalQuery;

  adapter.set("filters", {
    a: "A_ID",
    b: "B_ID",
  });

  normalQuery = adapter.normalizeQuery({a: 1, b: 2, c: 3, d: 4});

  assert.deepEqual(normalQuery.primaryFilter, 'A_ID:"1"');
  assert.deepEqual(normalQuery.secondaryFilter, 'B_ID:"2"');
  assert.deepEqual(normalQuery.c, 3);
  assert.deepEqual(normalQuery.d, 4);
});

test('query test', function(assert) {
  let adapter = this.subject(),
      normalQuery = {},
      testStore = {},
      testType = "ts",
      testQuery = {};

  assert.expect(1 + 1);

  adapter.normalizeQuery = function (params) {
    assert.equal(params, testQuery);
    return normalQuery;
  };
  adapter._loaderAjax = function (url, queryParams) {
    assert.equal(queryParams, normalQuery);
  };

  adapter.query(testStore, testType, {
    params: testQuery
  });
});
