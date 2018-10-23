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
import tableJoins from '../../../utils/table-joins';
import commons from '../../../mixins/commons';
import primaryDuration from '../../../configs/report-range-primary';
import secondaryDuration from '../../../configs/report-range-secondary';
import joinAlgorithms from '../../../configs/join-algorithms';

export default Ember.Route.extend(commons, {

  breadCrumb: {
    'title': 'Join',
    'linkable': false
   },

  alldatabases: null,
  selectedDb: null,

  selectedJoinAlgorithm: {label: 'All Join Algorithms', allAlgorithms: true},

  reportDateRange: Ember.inject.service('report-date-range'),

  reportsCountDatabase: Ember.inject.service('reports-count-database'),

  primaryDuration: primaryDuration,
  secondaryDuration: secondaryDuration,
  joinAlgorithms: joinAlgorithms,

  allDurations: Ember.computed('primaryDuration','secondaryDuration', function(){
    return this.get('primaryDuration').concat(this.get('secondaryDuration'));
  }),

  selectedDuration: Ember.computed('allDurations', function(){
    return this.get('allDurations').filterBy('default', true).get('firstObject');
  }),

  model(params, transition){
    //return dummyJoinList;

    return this.store.findAll('database').then((data) => {

      let tempAlldatabases =Ember.A();
      let database = transition.params["reports.database"] ? transition.params["reports.database"].database : undefined;
      let selectedDBFromQueryParams;

      data.forEach((item, index) => {
        let tempDb = {};
        if(!!item.get('name')){
          if(database === item.get('name')) {
            selectedDBFromQueryParams = item;
          }
          tempDb['id'] = item.get('id');
          tempDb['name'] = item.get('name');
          tempAlldatabases.push(tempDb);
        }
      });

      this.set('alldatabases', tempAlldatabases.sortBy('name'));
      this.set('selectedDb', selectedDBFromQueryParams || tempAlldatabases.filterBy('name', 'default').get('firstObject') || tempAlldatabases.get('firstObject'));
      this.set('filter', params.filter);

      let filteredDuration = this.get('allDurations').filterBy('id', params.filter);

      let dbId = this.get('selectedDb.id');
      let dbName = this.get('selectedDb.name');
      let duration = filteredDuration.length ? filteredDuration.get('firstObject') : this.get('selectedDuration');
      this.set("selectedDuration", duration);
      let range = this.get('reportDateRange').getDateRange(duration);
      let endDate = range.endDate.format('YYYY-MM-DD');
      let startDate = range.startDate.format('YYYY-MM-DD');
      let groupBy = this.get('selectedDuration.groupBy');


      let algorithm = params.algorithm;
      let algorithmMapped = this.get('joinAlgorithms').filterBy('value', algorithm).get('firstObject');
      if(algorithm === '' || algorithm === 'All Join Algorithms') {
        algorithmMapped = this.get('joinAlgorithms').get('firstObject');
      }
      this.set('selectedJoinAlgorithm', algorithmMapped);

      return this.get( 'reportsCountDatabase').fetchJoinReports(dbId , {startDate: startDate, endDate: endDate, groupedBy: groupBy, algorithm: algorithmMapped.value })

    });
  },

  setupController(controller, model) {
    this._super(...arguments);
    this.logGA('REPORTS_JOIN');
    controller.set('isLoading', false);

    controller.set('primaryDuration',this.get('primaryDuration'));
    controller.set('secondaryDuration',this.get('secondaryDuration'));
    controller.set('allDurations',this.get('allDurations'));
    controller.set('selectedDuration',this.get('selectedDuration'));

    let joinModel =  tableJoins(model, this.get('selectedDb.name'));
    controller.set('model', joinModel.finalJoinList);

    controller.set('selectedJoinAlgorithm', this.get('selectedJoinAlgorithm'));

    try {
      controller.set('joinAlgorithms', this.get('joinAlgorithms'));
    } catch(e) {
      controller.set('joinAlgorithms', [controller.get('selectedJoinAlgorithm')]);
    }
    controller.set('alldatabases',this.get('alldatabases').sortBy('name'));
    controller.set('selectedDb',this.get('selectedDb'));
    controller.set('filter',this.get('filter'));

  },
  actions:{
    reloadData(isSameRoute){
      let dbId = this.get('selectedDb.id');
      let dbName = this.get('selectedDb.name');
      let duration = this.get('selectedDuration');

      let range = this.get('reportDateRange').getDateRange(duration);
      let endDate = range.endDate.format('YYYY-MM-DD');
      let startDate = range.startDate.format('YYYY-MM-DD');
      let groupBy = this.get('selectedDuration.groupBy');
      let selectedJoinAlgorithm = this.get('selectedJoinAlgorithm');
      let algorithmParam = this.get('selectedJoinAlgorithm') && this.get('selectedJoinAlgorithm').allAlgorithms == true ? '' : this.get('selectedJoinAlgorithm').value;

      if(isSameRoute) {
      this.get( 'reportsCountDatabase').fetchJoinReports(dbId , {startDate: startDate, endDate: endDate, groupedBy: groupBy, algorithm: algorithmParam }).then((data) => {
        let joinModel =  tableJoins(data, dbName);
        this.get('controller').set('model', joinModel.finalJoinList);
        this.get('controller').set('isLoading', false);
       });
      }

      this.transitionTo('reports.database.joins', dbName, { queryParams: { filter: this.get('selectedDuration.id'), algorithm : algorithmParam}});

    },

    handleDbChange(selection, list){
      console.log('You have changed the DB.');
      this.get('controller').set('isLoading', true);
      let selectedDb = {id: selection.id, name: selection.name};
      this.set('selectedDb', selectedDb);
      this.get('controller').set('selectedDb',this.get('selectedDb'));
      this.send('reloadData');
    },

    handleDurationChange (selection, list){
      this.get('controller').set('isLoading', true);
      console.log('You have changed the duration.');
      let selectedDuration = selection;
      this.set('selectedDuration', selectedDuration);
      this.get('controller').set('selectedDuration', selectedDuration);
      this.send('reloadData', true);
    },

    handleAlgorithmChange(selection, list){

      console.log('You have changed the algorithm.');
      this.get('controller').set('isLoading', true);
      let selectedJoinAlgorithm = selection;
      this.set('selectedJoinAlgorithm', selectedJoinAlgorithm);
      this.get('controller').set('selectedJoinAlgorithm',this.get('selectedJoinAlgorithm'));
      this.send('reloadData', true);

    }
  }

});
