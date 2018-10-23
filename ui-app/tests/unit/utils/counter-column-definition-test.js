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

import CounterColumnDefinition from '../../../utils/counter-column-definition';
import { module, test } from 'qunit';

module('Unit | Utility | counter column definition');

test('Basic creation test', function(assert) {
  let definition = CounterColumnDefinition.create();

  assert.ok(definition);

  assert.ok(definition.getCellContent);
  assert.ok(definition.getSearchValue);
  assert.ok(definition.getSortValue);

  assert.ok(definition.id);
  assert.ok(definition.groupDisplayName);
  assert.ok(definition.headerTitle);

  assert.ok(CounterColumnDefinition.make);

  assert.equal(definition.observePath, true);
  assert.equal(definition.contentPath, "counterGroupsHash");
});

test('getCellContent, getSearchValue & getSortValue test', function(assert) {
  let testGroupName = "t.gn",
      testCounterName = "cn",
      testCounterValue = "val",
      testContent = {},
      testRow = {
        counterGroupsHash: testContent
      };

  testContent[testGroupName] = {};
  testContent[testGroupName][testCounterName] = testCounterValue;
  testContent[testGroupName]["anotherName"] = "anotherValue";

  let definition = CounterColumnDefinition.create({
    counterGroupName: testGroupName,
    counterName: testCounterName,
  });

  assert.equal(definition.getCellContent(testRow), testCounterValue);
  assert.equal(definition.getSearchValue(testRow), testCounterValue);
  assert.equal(definition.getSortValue(testRow), testCounterValue);
});

test('id test', function(assert) {
  let testGroupName = "t.gn",
      testCounterName = "cn";

  let definition = CounterColumnDefinition.create({
    counterGroupName: testGroupName,
    counterName: testCounterName,
  });

  assert.equal(definition.get("id"), `${testGroupName}/${testCounterName}`);
});

test('groupDisplayName test', function(assert) {
  let definition = CounterColumnDefinition.create();

  definition.set("counterGroupName", "foo");
  assert.equal(definition.get("groupDisplayName"), "foo");

  definition.set("counterGroupName", "foo.bar");
  assert.equal(definition.get("groupDisplayName"), "bar");

  definition.set("counterGroupName", "org.apache.tez.common.counters.DAGCounter");
  assert.equal(definition.get("groupDisplayName"), "DAG");

  definition.set("counterGroupName", "org.apache.tez.common.counters.FileSystemCounter");
  assert.equal(definition.get("groupDisplayName"), "FileSystem");

  definition.set("counterGroupName", "TaskCounter_ireduce1_INPUT_map");
  assert.equal(definition.get("groupDisplayName"), "Task - ireduce1 to Input-map");

  definition.set("counterGroupName", "TaskCounter_ireduce1_OUTPUT_reduce");
  assert.equal(definition.get("groupDisplayName"), "Task - ireduce1 to Output-reduce");
});

test('headerTitle test', function(assert) {
  let testGroupName = "t.gn",
      testCounterName = "cn";

  let definition = CounterColumnDefinition.create({
    counterGroupName: testGroupName,
    counterName: testCounterName,
  });

  assert.equal(definition.get("headerTitle"), "gn - cn");
});

test('CounterColumnDefinition.make test', function(assert) {
  var definitions = CounterColumnDefinition.make([{
    counterGroupName: "gn1",
    counterName: "nm1",
  }, {
    counterGroupName: "gn2",
    counterName: "nm2",
  }]);

  assert.equal(definitions.length, 2);
  assert.equal(definitions[0].get("headerTitle"), "gn1 - nm1");
  assert.equal(definitions[1].get("headerTitle"), "gn2 - nm2");
});
