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

moduleFor('serializer:loader', 'Unit | Serializer | loader', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:loader']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer._isLoader);

  assert.ok(serializer.extractId);
  assert.ok(serializer.extractAttributes);
  assert.ok(serializer.extractRelationships);

  assert.ok(serializer.extractSinglePayload);
  assert.ok(serializer.extractArrayPayload);

  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.normalizeArrayResponse);
});

test('extractId test', function(assert) {
  let serializer = this.subject(),
    modelClass = {},
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        entityID: 3
      }
    };

  assert.equal(serializer.extractId(modelClass, resourceHash), "ns:1", "With name-space");
  assert.equal(serializer.extractId(modelClass, { data: {id: 2} }), 2, "Without name-space");

  serializer.primaryKey = "entityID";
  assert.equal(serializer.extractId(modelClass, resourceHash), "ns:3", "Different primary key");
});

test('extractAttributes test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: function (callback) {
        callback("id", {type: "string"});
        callback("appID", {type: "string"});
        callback("status", {type: "string"});
      }
    },
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        appID: 2,
        applicationID: 3,
        info: {
          status: "SUCCESS"
        }
      }
    };

  assert.deepEqual(serializer.extractAttributes(modelClass, resourceHash), {
    id: 1,
    appID: 2
  });

  serializer.maps = {
    id: "id",
    appID: "applicationID",
    status: "info.status"
  };

  assert.deepEqual(serializer.extractAttributes(modelClass, resourceHash), {
    id: 1,
    appID: 3,
    status: "SUCCESS"
  });
});

test('extractRelationships test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: Ember.K,
      eachRelationship: function (callback) {
        callback("app", {
          key: "app",
          kind: "belongsTo",
          options: {},
          parentType: "parent",
          type: "app"
        });
      },
      eachTransformedAttribute: Ember.K
    },
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        app: "",
      }
    };

  assert.deepEqual(serializer.extractRelationships(modelClass, resourceHash), {
    app: {
      data: {
        id: null,
        type:"app"
      }
    }
  });

});

test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: function (callback) {
        callback("id", {type: "string"});
        callback("appID", {type: "string"});
        callback("status", {type: "string"});
      },
      eachRelationship: Ember.K,
      eachTransformedAttribute: Ember.K
    },
    resourceHash = {
      nameSpace: "ns",
      data: {
        id: 1,
        appID: 2,
        applicationID: 3,
        info: {
          status: "SUCCESS"
        }
      }
    };

  var response = serializer.normalizeSingleResponse({}, modelClass, resourceHash, null, null);

  assert.equal(response.data.id, "ns:1");
  assert.equal(response.data.attributes.id, 1);
  assert.equal(response.data.attributes.appID, 2);
});

test('normalizeArrayResponse test', function(assert) {
  let serializer = this.subject(),
    modelClass = {
      eachAttribute: function (callback) {
        callback("id", {type: "string"});
        callback("appID", {type: "string"});
        callback("status", {type: "string"});
      },
      eachRelationship: Ember.K,
      eachTransformedAttribute: Ember.K
    },
    resourceHash = {
      nameSpace: "ns",
      data: [{
        id: 1,
        appID: 2,
      },{
        id: 2,
        appID: 4,
      }]
    };

  var response = serializer.normalizeArrayResponse({}, modelClass, resourceHash, null, null);

  assert.equal(response.data.length, 2);
  assert.deepEqual(response.data[0].id, "ns:1");
  assert.deepEqual(response.data[1].id, "ns:2");
});
