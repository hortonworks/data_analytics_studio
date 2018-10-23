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

import moment from 'moment';

moduleForComponent('tab-n-refresh', 'Integration | Component | tab n refresh', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{tab-n-refresh}}`);

  assert.equal(this.$(".refresh-ui button").text().trim(), 'Refresh');
  assert.equal(
    this.$(".refresh-ui .text-elements").text().replace(/\n/g, "").trim().split(" ").slice(-4).join(" "),
    "Load time not available!"
  );
  assert.equal(this.$(".refresh-ui input").val(), 'on');

  this.render(hbs`
    {{#tab-n-refresh}}
      template block text
    {{/tab-n-refresh}}
  `);

  assert.equal(this.$(".refresh-ui button").text().replace(/\n/g, "").trim(), 'Refresh');
});

test('normalizedTabs test', function(assert) {
  var testTabs = [{
    text: "Tab 1",
    routeName: "route_1",
  },{
    text: "Tab 2",
    routeName: "route_2",
  }];

  this.set("tabs", testTabs);

  this.render(hbs`{{tab-n-refresh tabs=tabs}}`);

  assert.equal($(this.$("li")[0]).text().trim(), testTabs[0].text);
  assert.equal($(this.$("li")[1]).text().trim(), testTabs[1].text);
});

test('loadTime test', function(assert) {
  var loadTime = 1465226174574,
      timeInText = moment(loadTime).format("DD MMM YYYY HH:mm:ss");

  this.set("loadTime", loadTime);

  this.render(hbs`{{tab-n-refresh loadTime=loadTime}}`);
  assert.equal(
    this.$(".refresh-ui .text-elements").text().replace(/\n/g, "").trim().split(" ").slice(-7).join(" "),
    `Last refreshed at ${timeInText}`
  );
});
