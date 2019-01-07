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

import Ember from 'ember';

moduleForComponent('pagination-ui', 'Integration | Component | pagination ui', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("rowCountOptions", {
    rowCountOptions: [1, 2]
  });

  this.render(hbs`{{pagination-ui rowCountOptions=rowCountOptions}}`);

  assert.equal(this.$('select').length, 1);

  assert.equal(this.$('.page-list').length, 1);
  assert.equal(this.$('li').length, 0);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#pagination-ui rowCountOptions=rowCountOptions}}
      template block text
    {{/pagination-ui}}
  `);

  assert.equal(this.$('select').length, 1);
});

test('Page list test', function(assert) {
  this.set("tableDefinition", {
    pageNum: 5,
    rowCount: 5,

    loadingMore: false,
    moreAvailable: true,

    rowCountOptions: []
  });
  this.set("processor", {
    totalPages: 10,
    processedRows: {
      length: 10
    }
  });

  this.render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

  return wait().then(() => {
    assert.equal(this.$('li').length, 4);
    assert.equal(this.$('li').eq(0).text().trim(), "First");
    assert.equal(this.$('li').eq(1).text().trim(), "4");
    assert.equal(this.$('li').eq(2).text().trim(), "5");
    assert.equal(this.$('li').eq(3).text().trim(), "6");
  });
});

test('Page list - moreAvailable false test', function(assert) {
  this.set("tableDefinition", {
    pageNum: 5,
    rowCount: 5,

    loadingMore: false,
    moreAvailable: false,

    rowCountOptions: []
  });
  this.set("processor", {
    totalPages: 5,
    processedRows: {
      length: 10
    }
  });

  this.render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

  return wait().then(() => {
    assert.equal(this.$('li').length, 4);
    assert.equal(this.$('li').eq(1).text().trim(), "3");
    assert.equal(this.$('li').eq(2).text().trim(), "4");
    assert.equal(this.$('li').eq(3).text().trim(), "5");
  });
});

test('Page list - moreAvailable true test', function(assert) {
  this.set("tableDefinition", {
    pageNum: 5,
    rowCount: 5,

    loadingMore: false,
    moreAvailable: true,

    rowCountOptions: []
  });
  this.set("processor", {
    totalPages: 5,
    processedRows: {
      length: 10
    }
  });

  this.render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

  return wait().then(() => {
    assert.equal(this.$('li').length, 4);
    assert.equal(this.$('li').eq(1).text().trim(), "4");
    assert.equal(this.$('li').eq(2).text().trim(), "5");
    assert.equal(this.$('li').eq(3).text().trim(), "6");
  });
});

test('No data test', function(assert) {
  var customRowCount = 2,
      definition = {
        rowCount: customRowCount,
        loadingMore: false,
        moreAvailable: true,

        rowCountOptions: []
      },
      processor;

  Ember.run(function () {
    processor = {
      tableDefinition: definition,
      rows: Ember.A()
    };
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 0);
});