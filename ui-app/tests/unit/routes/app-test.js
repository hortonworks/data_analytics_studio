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

moduleFor('route:app', 'Unit | Route | app', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('it exists', function(assert) {
  let route = this.subject();
  assert.ok(route);
});

test('Test model - Without app data', function(assert) {
  let testID = "123",
      route = this.subject({
        loader: {
          queryRecord: function (type, id) {
            assert.ok(type === 'AhsApp' || type === 'appRm');
            assert.equal(id, testID);
            return {
              catch: function (callBack) {
                return callBack();
              }
            };
          }
        }
      }),
      data;

  assert.expect(2 + 2 + 1);

  data = route.model({
    "app_id": testID
  });
  assert.equal(data.get("entityID"), testID);
});

test('Test model - With app data', function(assert) {
  let testID1 = "123",
      testData1 = {},
      testID2 = "456",
      testData2 = {},
      route = this.subject({
        loader: {
          queryRecord: function (type, id) {
            if(id === "123"){
              assert.equal(type, 'AhsApp');
              return {
                catch: function () {
                  return testData1;
                }
              };
            }
            else if(id === "456") {
              if(type === "AhsApp") {
                return {
                  catch: function (callBack) {
                    return callBack();
                  }
                };
              }
              assert.equal(type, 'appRm');
              return {
                catch: function () {
                  return testData2;
                }
              };
            }
          }
        }
      }),
      data;

  assert.expect(2 + 2);

  data = route.model({
    "app_id": testID1
  });
  assert.equal(data, testData1);

  data = route.model({
    "app_id": testID2
  });
  assert.equal(data, testData2);
});
