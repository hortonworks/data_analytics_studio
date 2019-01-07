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

import Process from '../../../utils/process';
import { module, test } from 'qunit';

module('Unit | Utility | process');

test('Basic creation test', function(assert) {
  let process = Process.create();

  assert.ok(process);

  assert.ok(process.consolidateStartTime);
  assert.ok(process.consolidateEndTime);

  assert.ok(process.init);

  assert.ok(process.getBarColor);
  assert.ok(process.getConsolidateColor);

  assert.ok(process.getColor);
  assert.ok(process.startEvent);
  assert.ok(process.endEvent);
  assert.ok(process.getAllBlockers);
  assert.ok(process.getTooltipContents);
});

test('_id test', function(assert) {
  let nextID = parseInt(Process.create().get("_id").split("-")[2]) + 1;

  let process = Process.create();
  assert.equal(process.get("_id"), "process-id-" + nextID);
});


test('getColor test', function(assert) {
  let process = Process.create();

  assert.equal(process.getColor(), "#0");

  process.set("color", {
    h: 10,
    s: 20,
    l: 30
  });
  assert.equal(process.getColor(), "hsl( 10, 20%, 30% )");
  assert.equal(process.getColor(0.2), "hsl( 10, 20%, 40% )");
});

test('startEvent test', function(assert) {
  let process = Process.create();

  assert.equal(process.get("startEvent"), undefined);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 20,
  }, {
    time: 80,
  }]);
  assert.equal(process.get("startEvent").time, 20);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 80,
  }]);
  assert.equal(process.get("startEvent").time, 50);
});

test('endEvent test', function(assert) {
  let process = Process.create();

  assert.equal(process.get("endEvent"), undefined);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 20,
  }, {
    time: 80,
  }]);
  assert.equal(process.get("endEvent").time, 80);

  process.set("events", [{
    time: 50,
  }, {
    time: 70,
  }, {
    time: 20,
  }]);
  assert.equal(process.get("endEvent").time, 70);
});

test('getAllBlockers test', function(assert) {
  var cyclicProcess = Process.create({
    name: "p3",
  });
  cyclicProcess.blockers = [cyclicProcess];

  var multiLevelCycle1 = Process.create({
    name: "p5",
  });
  var multiLevelCycle2 = Process.create({
    name: "p6",
  });
  multiLevelCycle1.blockers = [multiLevelCycle2];
  multiLevelCycle2.blockers = [multiLevelCycle1];

  var process = Process.create({
    blockers: [Process.create({
      name: "p1"
    }), Process.create({
      name: "p2",
      blockers: [Process.create({
        name: "p21"
      }), Process.create({
        name: "p22",
        blockers: [Process.create({
          name: "p221"
        })]
      })]
    }), cyclicProcess, Process.create({
      name: "p4"
    }), multiLevelCycle1]
  });

  var all = process.getAllBlockers();

  assert.equal(all.length, 9);

  assert.equal(all[0].get("name"), "p1");
  assert.equal(all[1].get("name"), "p2");
  assert.equal(all[2].get("name"), "p21");
  assert.equal(all[3].get("name"), "p22");
  assert.equal(all[4].get("name"), "p221");
  assert.equal(all[5].get("name"), "p3");
  assert.equal(all[6].get("name"), "p4");
  assert.equal(all[7].get("name"), "p5");
  assert.equal(all[8].get("name"), "p6");

});
