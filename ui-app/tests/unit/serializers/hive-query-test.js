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

moduleFor('serializer:hive-query', 'Unit | Serializer | hive query', {
  // Specify the other units that are required for this test.
  needs: ['model:hive-query']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();
  assert.equal(Object.keys(serializer.get("maps")).length, 6 + 21);
  assert.ok(serializer.get("extractAttributes"));
});

test('getStatus test', function(assert) {
  let serializer = this.subject(),
      getStatus = serializer.get("maps.status");

  assert.equal(getStatus({}), "RUNNING");
  assert.equal(getStatus({
    otherinfo: {
      STATUS: true
    }
  }), "SUCCEEDED");
  assert.equal(getStatus({
    otherinfo: {
      STATUS: false
    }
  }), "FAILED");
});

test('getEndTime test', function(assert) {
  let serializer = this.subject(),
      getEndTime = serializer.get("maps.endTime"),
      endTime = 23;

  assert.equal(getEndTime({}), undefined);

  assert.equal(getEndTime({
    otherinfo: {
      endTime: endTime
    }
  }), endTime);

  assert.equal(getEndTime({
    events: [{
      eventtype: 'X',
    }, {
      eventtype: 'QUERY_COMPLETED',
      timestamp: endTime
    }, {
      eventtype: 'Y',
    }]
  }), endTime);
});

test('extractAttributes test', function(assert) {
  let serializer = this.subject(),
      testQuery = {
        abc: 1,
        xyz: 2
      },
      testHiveAddress = "1.2.3.4",
      testData = {
        otherinfo: {
          QUERY: JSON.stringify(testQuery),
          HIVE_ADDRESS: testHiveAddress
        }
      };

  serializer.extractAttributes(Ember.Object.create({
    eachAttribute: Ember.K
  }), {
    data: testData
  });
  assert.deepEqual(testData.otherinfo.QUERY, testQuery);

  //CLIENT_IP_ADDRESS set
  assert.equal(testHiveAddress, testData.otherinfo.CLIENT_IP_ADDRESS);

  // Tables read & tables written
  assert.ok(testData.primaryfilters);
  assert.ok(testData.primaryfilters.tablesread instanceof Error);
  assert.ok(testData.primaryfilters.tableswritten instanceof Error);
  assert.equal(testData.primaryfilters.tablesread.message, "None");
  assert.equal(testData.primaryfilters.tableswritten.message, "None");
});
