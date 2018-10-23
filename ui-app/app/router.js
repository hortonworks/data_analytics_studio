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
import config from './config/environment';

const Router = Ember.Router.extend({
  location: config.locationType,
  rootURL: config.rootURL
});
Router.map(function() {
  // IDE
  this.route('jobs');

  this.route('savedqueries');

  this.route('messages', function() {
    this.route('message', {path: '/:message_id'});
  });
  this.route('queries', function() {
    this.route('new');
    this.route('query', {path: '/:worksheetId'}, function() {
      this.route('results');
      this.route('log');
      this.route('visual-explain');
      this.route('tez-ui');
      this.route('loading');
      this.route('error-log');
    });
  });

  this.route('databases', function() {
    this.route('newtable');
    this.route('database', {path: '/:databaseId'}, function() {
      this.route('tables', {path: '/tables'}, function() {
        this.route('new-database');
        this.route('new');
        this.route('upload-table');
        this.route('table', {path: '/:name'}, function() {
          this.route('edit');
          this.route('rename');
          this.route('columns');
          this.route('partitions');
          this.route('storage');
          this.route('details');
          this.route('view');
          this.route('ddl');
          this.route('stats');
          this.route('auth');
          this.route('data-preview');
        });
      });
    });
    this.route('list');
  });

  //Search
  this.route('hive-queries', {path: "/"});

  this.route('dag', {path: '/dag/:dag_id'}, function() {
    this.route('vertices');
    this.route('tasks');
    this.route('attempts');
    this.route('counters');
    this.route('index', {path: '/'}, function() {});
    this.route('graphical');
    this.route('swimlane');
  });
  this.route('vertex', {path: '/vertex/:vertex_id'}, function() {
    this.route('tasks');
    this.route('attempts');
    this.route('counters');
    this.route('configs');
  });
  this.route('task', {path: '/task/:task_id'}, function() {
    this.route('attempts');
    this.route('counters');
  });
  this.route('attempt', {path: '/attempt/:attempt_id'}, function () {
    this.route('counters');
  });
  this.route('query', {path: '/query/:query_id'}, function() {
    this.route('configs');
    this.route('timeline');
    this.route('recommendations');
    this.route('visual-explain');
    this.route('dag');
  });

  this.route('app', {path: '/app/:app_id'}, function () {
    this.route('dags');
    this.route('configs');
  });

  // Reports
  if(config.APP.DASLITE == false) {
    this.route('reports', function() {
      this.route('database', { path: '/:database' }, function() {
        this.route('joins', { path: '/joins' });
        this.route('database-read', function() {
            this.route('relations', { path: '/relations/:table_id' });
        });
      });
  
      this.route('database-write');
      this.route('database-storage');
    });
  }
  

  if(config.APP.DISABLE_UDF_SETTINGS != true) {
    this.route('udfs', function() {
      this.route('new');
      this.route('udf', {path: '/:udfId'}, function() {
      });
      this.route('edit', {path: 'edit/:udfId'});
    });
    this.route('settings');
  }


  if(config.APP.CLOUD == true) {
    this.route('warehouses', function() {
      this.route('create');
    });
  }
});

export default Router;
