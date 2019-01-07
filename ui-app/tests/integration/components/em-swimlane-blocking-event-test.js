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

moduleForComponent('em-swimlane-blocking-event', 'Integration | Component | em swimlane blocking event', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("process", Process.create());
  this.set("processor", Processor.create());

  this.render(hbs`{{em-swimlane-blocking-event processor=processor process=process}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane-blocking-event processor=processor process=process}}
      template block text
    {{/em-swimlane-blocking-event}}
  `);

  assert.equal(this.$().text().trim(), '');
});

test('Blocking test', function(assert) {
  var blockingEventName = "blockingEvent",
      processIndex = 5,
      blockingIndex = 7,
      processColor = "#123456";

  this.set("process", Process.create({
    blockingEventName: blockingEventName,
    index: processIndex,
    getColor: function () {
      return processColor;
    },
    events: [{
      name: blockingEventName,
      time: 2
    }]
  }));
  this.set("blocking", Process.create({
    index: blockingIndex,
    endEvent: {
      time: 5
    }
  }));
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 10
  }));

  this.render(hbs`{{em-swimlane-blocking-event processor=processor process=process blocking=blocking}}`);

  return wait().then(() => {
    assert.equal(this.$(".em-swimlane-blocking-event").attr("style").trim(), 'left: 20%;');
    assert.equal(this.$(".event-line").css("height"), ((blockingIndex - processIndex) * 30) + "px");
  });
});

test('Blocking test with blocking.endEvent.time < blockTime', function(assert) {
  var blockingEventName = "blockingEvent",
      processIndex = 5,
      blockingIndex = 7,
      processColor = "#123456";

  this.set("process", Process.create({
    blockingEventName: blockingEventName,
    index: processIndex,
    getColor: function () {
      return processColor;
    },
    events: [{
      name: blockingEventName,
      time: 5
    }]
  }));
  this.set("blocking", Process.create({
    index: blockingIndex,
    endEvent: {
      time: 2
    }
  }));
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 10
  }));

  this.render(hbs`{{em-swimlane-blocking-event processor=processor process=process blocking=blocking}}`);

  return wait().then(() => {
    assert.equal(this.$(".em-swimlane-blocking-event").attr("style"), undefined);
  });
});
