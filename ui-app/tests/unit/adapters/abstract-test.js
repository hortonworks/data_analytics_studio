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

moduleFor('adapter:abstract', 'Unit | Adapter | abstract', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:foo']
});

test('Basic creation test', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.equal(adapter.serverName, null);

  assert.ok(adapter.host);
  assert.ok(adapter.namespace);
  assert.ok(adapter.pathTypeHash);

  assert.ok(adapter.ajaxOptions);
  assert.ok(adapter.pathForType);

  assert.ok(adapter.normalizeErrorResponse);
  assert.ok(adapter._loaderAjax);
});

test('host, namespace & pathTypeHash test', function(assert) {
  let adapter = this.subject(),
      testServerName = "sn",
      testHosts = {
        sn: "foo.bar",
      },
      testENV = {
        app: {
          namespaces: {
            webService: {
              sn: "ws"
            }
          },
          paths: {
            sn: "path"
          }
        }
      };

  adapter.hosts = testHosts;
  adapter.env = testENV;
  adapter.set("serverName", testServerName);

  assert.equal(adapter.get("host"), testHosts.sn);
  assert.equal(adapter.get("namespace"), testENV.app.namespaces.webService.sn);
  assert.equal(adapter.get("pathTypeHash"), testENV.app.paths.sn);
});

test('ajaxOptions test', function(assert) {
  let adapter = this.subject(),
      testUrl = "foo.bar",
      testMethod = "tm",
      testOptions = {
        a: 1
      },
      testServer = "ts",

      result;

  // Without options
  adapter.serverName = testServer;
  result = adapter.ajaxOptions(testUrl, testMethod);
  assert.ok(result);
  assert.ok(result.crossDomain);
  assert.ok(result.xhrFields.withCredentials);
  assert.equal(result.targetServer, testServer);

  // Without options
  adapter.serverName = testServer;
  result = adapter.ajaxOptions(testUrl, testMethod, testOptions);
  assert.ok(result);
  assert.ok(result.crossDomain);
  assert.ok(result.xhrFields.withCredentials);
  assert.equal(result.targetServer, testServer);
  assert.equal(result.a, testOptions.a);
});

test('pathForType test', function(assert) {
  let adapter = this.subject(),
      testHash = {
        typ: "type"
      };

  assert.expect(2);

  adapter.pathTypeHash = testHash;
  assert.equal(adapter.pathForType("typ"), testHash.typ);
  assert.throws(function () {
    adapter.pathForType("noType");
  });
});

test('normalizeErrorResponse test', function(assert) {
  let adapter = this.subject(),
      status = "200",
      testTitle = "title",
      strPayload = "StringPayload",
      objPayload = {x: 1, message: testTitle},
      testHeaders = {},
      response;

  response = adapter.normalizeErrorResponse(status, testHeaders, strPayload);
  assert.equal(response[0].title, undefined);
  assert.equal(response[0].status, status);
  assert.equal(response[0].detail, strPayload);
  assert.equal(response[0].headers, testHeaders);

  response = adapter.normalizeErrorResponse(status, testHeaders, objPayload);
  assert.equal(response[0].title, testTitle);
  assert.equal(response[0].status, status);
  assert.deepEqual(response[0].detail, objPayload);
  assert.equal(response[0].headers, testHeaders);
});

test('normalizeErrorResponse html payload test', function(assert) {
  let adapter = this.subject(),
      status = "200",
      htmlPayload = "StringPayload <b>boldText</b> <script>scriptText</script> <style>styleText</style>",
      testHeaders = {},
      response;

  response = adapter.normalizeErrorResponse(status, testHeaders, htmlPayload);
  assert.equal(response[0].detail, "StringPayload boldText");
});

test('_loaderAjax resolve test', function(assert) {
  let result = {},
      adapter = this.subject({
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.resolve(result);
        }
      });

  assert.expect(1 + 1);

  adapter._loaderAjax().then(function (val) {
    assert.equal(val.data, result);
  });
});

test('_loaderAjax reject, without title test', function(assert) {
  let errorInfo = {
        status: "500",
        detail: "testDetails"
      },
      msg = "Msg",
      testUrl = "http://foo.bar",
      testQuery = {},
      testNS = "namespace",
      adapter = this.subject({
        outOfReachMessage: "OutOfReach",
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.reject({
            message: msg,
            errors:[errorInfo]
          });
        }
      });

  assert.expect(1 + 7);

  adapter._loaderAjax(testUrl, testQuery, testNS).catch(function (val) {
    assert.equal(val.message, `${msg} » ${errorInfo.status}: Error accessing ${testUrl}`);
    assert.equal(val.details, errorInfo.detail);
    assert.equal(val.requestInfo.adapterName, "abstract");
    assert.equal(val.requestInfo.url, testUrl);

    assert.equal(val.requestInfo.queryParams, testQuery);
    assert.equal(val.requestInfo.namespace, testNS);

    assert.ok(val.requestInfo.hasOwnProperty("responseHeaders"));
  });
});

test('_loaderAjax reject, with title test', function(assert) {
  let errorInfo = {
        status: "500",
        title: "Server Error",
        detail: "testDetails"
      },
      msg = "Msg",
      testUrl = "url",
      adapter = this.subject({
        outOfReachMessage: "OutOfReach",
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.reject({
            message: msg,
            errors:[errorInfo]
          });
        }
      });

  assert.expect(1 + 5);

  adapter._loaderAjax(testUrl).catch(function (val) {
    assert.equal(val.message, `${msg} » ${errorInfo.status}: ${errorInfo.title}`);
    assert.equal(val.details, errorInfo.detail);
    assert.equal(val.requestInfo.adapterName, "abstract");
    assert.equal(val.requestInfo.url, testUrl);

    assert.ok(val.requestInfo.hasOwnProperty("responseHeaders"));
  });
});

test('_loaderAjax reject, status 0 test', function(assert) {
  let errorInfo = {
        status: 0,
        title: "Server Error",
        detail: "testDetails"
      },
      msg = "Msg",
      testUrl = "url",
      adapter = this.subject({
        outOfReachMessage: "OutOfReach",
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.reject({
            message: msg,
            errors:[errorInfo]
          });
        }
      });

  assert.expect(1 + 5);

  adapter._loaderAjax(testUrl).catch(function (val) {
    assert.equal(val.message, `${msg} » ${adapter.outOfReachMessage}`);
    assert.equal(val.details, errorInfo.detail);
    assert.equal(val.requestInfo.adapterName, "abstract");
    assert.equal(val.requestInfo.url, testUrl);

    assert.ok(val.requestInfo.hasOwnProperty("responseHeaders"));
  });
});
