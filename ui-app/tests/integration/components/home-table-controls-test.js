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

import Ember from 'ember';

moduleForComponent('home-table-controls', 'Integration | Component | home table controls', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{home-table-controls}}`);

  assert.equal(this.$().text().trim(), 'Load Counters');
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), 2);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#home-table-controls}}
      template block text
    {{/home-table-controls}}
  `);

  assert.equal(this.$().text().trim(), 'Load Counters');
});

test('countersLoaded test', function(assert) {
  this.set("dataProcessor", {
    processedRows: [Ember.Object.create({
      counterGroupsHash: {
        counter: {}
      }
    }), Ember.Object.create({
      counterGroupsHash: {
        counter: {}
      }
    })]
  });
  this.render(hbs`{{home-table-controls dataProcessor=dataProcessor}}`);
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), 2);

  this.set("dataProcessor", {
    processedRows: [Ember.Object.create({
      counterGroupsHash: {}
    }), Ember.Object.create({
      counterGroupsHash: {
        counter: {}
      }
    })]
  });
  this.render(hbs`{{home-table-controls dataProcessor=dataProcessor}}`);
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), 2);

  this.set("dataProcessor", {
    processedRows: [Ember.Object.create({
      counterGroupsHash: {}
    }), Ember.Object.create({
      counterGroupsHash: {}
    })]
  });
  this.render(hbs`{{home-table-controls dataProcessor=dataProcessor}}`);
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), -1);
});
