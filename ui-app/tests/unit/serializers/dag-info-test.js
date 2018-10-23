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

moduleFor('serializer:dag-info', 'Unit | Serializer | dag info', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:dag-info']
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);

  assert.ok(serializer.normalizeResourceHash);

  assert.ok(serializer.maps.dagPlan);
  assert.ok(serializer.maps.callerData);

  assert.equal(Object.keys(serializer.get("maps")).length, 2 + 7); //2 own & 7 inherited
});

test('normalizeResourceHash test', function(assert) {
  let serializer = this.subject(),

      callerInfo = {
        callerId: "id_1",
        callerType: "HIVE_QUERY_ID",
        context: "Hive",
        description: "hive query"
      },

      data;

  // dagContext test
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagContext: callerInfo
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.equal(data.callerData.callerType, callerInfo.callerType);

  // dagInfo test
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagInfo: `{"context": "${callerInfo.context}", "description": "${callerInfo.description}"}`
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.notOk(data.callerData.callerType);

  // dagInfo.blob test
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagInfo: {
            context: callerInfo.context,
            blob: callerInfo.description
          }
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.notOk(data.callerData.callerType);

  // dagContext have presidence over dagInfo
  data = serializer.normalizeResourceHash({
    data: {
      otherinfo: {
        dagPlan: {
          dagContext: callerInfo,
          dagInfo: `{"context": "RandomContext", "description": "RandomDesc"}`
        }
      }
    }
  }).data;

  assert.equal(data.callerData.callerContext, callerInfo.context);
  assert.equal(data.callerData.callerDescription, callerInfo.description);
  assert.equal(data.callerData.callerType, callerInfo.callerType);
});
