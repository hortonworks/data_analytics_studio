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

moduleFor('controller:table', 'Unit | Controller | table', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  });

  assert.ok(controller);
  assert.ok(controller.queryParams);

  assert.equal(controller.rowCount, 10);
  assert.equal(controller.searchText, "");
  assert.equal(controller.sortColumnId, "");
  assert.equal(controller.sortOrder, "");
  assert.equal(controller.pageNo, 1);

  assert.ok(controller.headerComponentNames);
  assert.ok(controller.visibleColumnIDs);
  assert.ok(controller.columnSelectorTitle);
  assert.ok(controller.definition);

  assert.ok(controller.storageID);
  assert.ok(controller.initVisibleColumns);

  assert.ok(controller.beforeSort);
  assert.ok(controller.columns);
  assert.ok(controller.allColumns);
  assert.ok(controller.visibleColumns);

  assert.ok(controller.getCounterColumns);

  assert.ok(controller.actions.searchChanged);
  assert.ok(controller.actions.sortChanged);
  assert.ok(controller.actions.rowsChanged);
  assert.ok(controller.actions.pageChanged);

  assert.ok(controller.actions.openColumnSelector);
  assert.ok(controller.actions.columnsSelected);
});

test('initVisibleColumns test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    localStorage: Ember.Object.create(),
    columns: []
  });

  controller.set("columns", [{
    id: "c1",
  }, {
    id: "c2",
  }, {
    id: "c3",
  }]);
  controller.initVisibleColumns();
  assert.equal(controller.get("visibleColumnIDs.c1"), true);
  assert.equal(controller.get("visibleColumnIDs.c2"), true);
  assert.equal(controller.get("visibleColumnIDs.c3"), true);

  controller.set("columns", [{
    id: "c1",
    hiddenByDefault: true,
  }, {
    id: "c2",
  }, {
    id: "c3",
    hiddenByDefault: true,
  }]);
  controller.initVisibleColumns();
  assert.equal(controller.get("visibleColumnIDs.c1"), false);
  assert.equal(controller.get("visibleColumnIDs.c2"), true);
  assert.equal(controller.get("visibleColumnIDs.c3"), false);

  controller.initVisibleColumns();
  assert.equal(controller.get("visibleColumnIDs.c1"), false);
  assert.equal(controller.get("visibleColumnIDs.c2"), true);
  assert.equal(controller.get("visibleColumnIDs.c3"), false);
});
