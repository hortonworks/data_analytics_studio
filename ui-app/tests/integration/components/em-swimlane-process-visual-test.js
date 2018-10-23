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

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

import wait from 'ember-test-helpers/wait';

import Process from 'hivestudio/utils/process';
import Processor from 'hivestudio/utils/processor';

moduleForComponent('em-swimlane-process-visual', 'Integration | Component | em swimlane process visual', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("process", Process.create());
  this.set("processor", Processor.create());

  this.render(hbs`{{em-swimlane-process-visual process=process processor=processor}}`);

  assert.ok(this.$(".base-line"));
  assert.ok(this.$(".event-window"));

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane-process-visual processor=processor process=process}}
      template block text
    {{/em-swimlane-process-visual}}
  `);

  assert.ok(this.$(".base-line"));
  assert.ok(this.$(".event-window"));
});

test('Events test', function(assert) {
  this.set("process", Process.create({
    events: [{
      name: "event1",
      time: 5
    }, {
      name: "event2",
      time: 7
    }]
  }));
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 10
  }));

  this.render(hbs`{{em-swimlane-process-visual processor=processor process=process startTime=0 timeWindow=10}}`);

  return wait().then(() => {
    var events = this.$(".em-swimlane-event");

    assert.equal(events.length, 2);
    assert.equal(events.eq(0).attr("style").trim(), "left: 50%;", "em-swimlane-event 1 left");
    assert.equal(events.eq(1).attr("style").trim(), "left: 70%;", "em-swimlane-event 2 left");

    assert.equal(this.$(".process-line").eq(0).attr("style").trim(), "left: 50%; right: 30%;", "process-line");
  });
});
