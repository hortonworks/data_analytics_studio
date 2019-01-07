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
import SingleAmPollsterRoute from '../single-am-pollster';
import queryDetailsTabs from '../../configs/query-details-tabs';
import commons from '../../mixins/commons';

export default SingleAmPollsterRoute.extend(commons, {

  breadCrumb: {
    title: '',
  },
  renderTemplate: function() {


    this.render();

    this.render('query.recommendations', {
      into: 'query.index',
      outlet: 'recommendations',
      controller: 'query.recommendations',
      model: this.controllerFor('query').get('hiveQueryId')
    });

    this.render('query.visual-explain', {
      into: 'query.index',
      outlet: 'visual-explain',
      controller: 'query.visual-explain'
    });

    this.render('query.timeline', {
      into: 'query.index',
      outlet: 'timeline',
      controller: 'query.timeline'
    });

    this.render('query.dag', {
      into: 'query.index',
      outlet: 'dag',
      controller: 'query.dag'
    });

    this.render('query.configs', {
      into: 'query.index',
      outlet: 'configs',
      controller: 'query.configs'
    });

    // DAG panel
    this.render('dag.swimlane', {
      into: 'query.index',
      outlet: 'dag-swimlane',
      controller: 'dag.swimlane'
    });
    this.render('dag.graphical', {
      into: 'query.index',
      outlet: 'dag-graphical-view',
      controller: 'dag.graphical'
    });
    this.render('dag.counters', {
      into: 'query.index',
      outlet: 'dag-counters',
      controller: 'dag.counters'
    });

  },

  title: "Query Details",

  loaderNamespace: "query",

  setupController: function (controller, model) {
    this._super(controller, model);
    this.logGA('QUERY_DETAILS');
    this.controllerFor('query.index').set('queryDetailsTabs', queryDetailsTabs);
    this.set('breadCrumb.title', this.getQueryDetailsPage(this.controllerFor('query').get('hiveQueryId')));
    let querymodel = this.modelFor("query");

    querymodel.set('tablesReadWithDatabase', querymodel.get('tablesRead').map(function (data) {
      return `${data.table} (${data.database})`;
    }).join(", "));

    querymodel.set( 'tablesWrittenWithDatabase', querymodel.get('tablesWritten').map(function (data) {
      return `${data.table} (${data.database})`;
    }).join(", "));

    this.controllerFor('query.index').set('querymodel', querymodel);
    this.controllerFor('query.visual-explain').set('model', querymodel);
    this.controllerFor('query.timeline').set('timelinemodel', querymodel);
    this.controllerFor('query.dag').set('dagmodel', querymodel);
    this.controllerFor('query.configs').set('configsmodel', querymodel);
    this.controllerFor('dag.counters').set('model', {
      counterGroupsHash: this.createCounterGroupsHash(querymodel.get("details.counters") || [])
    });
    let isDAGEmpty = Ember.$.isEmptyObject(this.controllerFor('dag.counters').get('model.counterGroupsHash')) && !(querymodel.get('executionMode') === 'LLAP' || querymodel.get('executionMode') === 'TEZ');
    let queryDetailsTabsMod = queryDetailsTabs;
    controller.set('isDAGEmpty', isDAGEmpty);
    if(isDAGEmpty) {
      queryDetailsTabsMod = queryDetailsTabsMod.filter(queryDetailsTab => queryDetailsTab.id !== "dag-panel");
    }
    controller.set('queryDetailsTabsMod', queryDetailsTabsMod);
    this.controllerFor('dag.counters').set('definition.searchText', '');
    if(this.controllerFor('query.configs').get('definition.searchText') !== 'hive.') {
      this.controllerFor('query.configs').set('definition.searchText', '');
    }

    var that = this;
    this.get("loader").query('vertex', {dagId: querymodel.get("dagInfo.dagId")}, {reload: true}).then(function (vertices) {
      vertices = that.createVerticesData(vertices, querymodel.get("dagInfo"), querymodel.get("details.dagPlan"));
      that.controllerFor('dag.swimlane').set('model', vertices);
      that.controllerFor('dag.graphical').set('model', vertices);
    }, function () {
      that.controllerFor('dag.swimlane').set('model', []);
      that.controllerFor('dag.graphical').set('model', []);
    });
  },

  isDAGComplete: function (status) {
    switch(status) {
      case "SUCCEEDED":
      case "FINISHED":
      case "FAILED":
      case "KILLED":
      case "ERROR":
        return true;
    }
    return false;
  },

  createVerticesData: function (vertices, dagInfo, dagPlan) {

    if (!dagPlan || !dagPlan.vertices) {
      return []
    }

    var dagObj = Ember.Object.create({
      amWsVersion: 2,
      isComplete: this.isDAGComplete(dagInfo.status),

      edges: dagPlan.edges,
      vertices: dagPlan.vertices,
    });

    vertices.forEach(function (vertex) {
      vertex.set("dag", dagObj);
    });

    return vertices;
  },


  createCounterGroupsHash: function(counterGroups) {
    var counterHash = {};

    counterGroups.forEach(function (group) {
      var counters = group.counters,
          groupHash;

      groupHash = counterHash[group.counterGroupName] = counterHash[group.counterGroupName] || {};

      counters.forEach(function (counter) {
        groupHash[counter.counterName] = counter.counterValue;
      });
    });

    return counterHash;
  },


  load: function (value, query, options) {
    //return this.get("loader").queryRecord('hive-query', this.modelFor("query").get("id"), options);
  }

});



