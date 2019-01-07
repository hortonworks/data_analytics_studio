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
import databaseReadsMocks from '../../../configs/database-reads-mocks';
import tableHeatmap from '../../../utils/table-heatmap';
import commons from '../../../mixins/commons';
import primaryDuration from '../../../configs/report-range-primary';
import secondaryDuration from '../../../configs/report-range-secondary';

export default Ember.Route.extend(commons, {

   breadCrumb: {
   'title': 'Read-Write',
   'linkable': false
  },

  alldatabases: null,
  selectedDb: null,

  reportsCountDatabase: Ember.inject.service('reports-count-database'),

  reportDateRange: Ember.inject.service('report-date-range'),

  primaryDuration: primaryDuration,
  secondaryDuration: secondaryDuration,

  allDurations: Ember.computed('primaryDuration','secondaryDuration', function(){
    return this.get('primaryDuration').concat(this.get('secondaryDuration'));
  }),

  selectedDuration: Ember.computed('allDurations', function(){
    return this.get('allDurations').filterBy('default', true).get('firstObject');
  }),

  model(params, transition){
    //return databaseReadsMocks;
    return this.store.findAll('database').then((data) => {

      let tempAlldatabases = Ember.A();
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
      this.set('selectedTableId', transition.params["reports.database.database-read.relations"] ? transition.params["reports.database.database-read.relations"].table_id : "");
      this.set('filter', params.filter);

      let filteredDuration = this.get('allDurations').filterBy('id', params.filter);
      let dbId = this.get('selectedDb.id');
      let dbName = this.get('selectedDb.name');
      let duration = filteredDuration.length ? filteredDuration.get('firstObject') : this.get('selectedDuration');
      let range = this.get('reportDateRange').getDateRange(duration);
      this.set("selectedDuration", duration);
      let endDate = range.endDate.format('YYYY-MM-DD');
      let startDate = range.startDate.format('YYYY-MM-DD');

      let groupBy = this.get('selectedDuration.groupBy');
      return this.get( 'reportsCountDatabase').fetchReport(dbId , {startDate: startDate, endDate: endDate, groupedBy: groupBy });

    });

  },

  filteredHeatTables:Ember.A(),

  renderTemplate: function() {
    this.render();
    Ember.run.later(()=>{
      let selectedTableId = this.get('selectedTableId') || this.get('filteredHeatTables').get('firstObject.tableId');
      this.transitionTo('reports.database.database-read.relations', selectedTableId, { queryParams: { filter: this.get('selectedDuration.id') }});
    }, 500);
  },

  setupController(controller, model) {
    this._super(...arguments);
    this.logGA('REPORTS_RW');
    controller.set('isLoading', false);

    controller.set('primaryDuration',this.get('primaryDuration'));
    controller.set('secondaryDuration',this.get('secondaryDuration'));
    controller.set('allDurations',this.get('allDurations'));
    controller.set('selectedDuration',this.get('selectedDuration'));

    controller.set('model', model);
    controller.set('tables', model.tables);
    controller.set('reports', model.reports);
    controller.set('heatTables', tableHeatmap(model));
    controller.set('filteredHeatTables', controller.get('heatTables'));
    this.set('filteredHeatTables', controller.get('heatTables'));

    controller.set('relationsRoute', false);
    controller.set('alldatabases',this.get('alldatabases').sortBy('name'));
    controller.set('selectedDb',this.get('selectedDb'));
    controller.set('selectedTableId',this.get('selectedTableId'));
    controller.set('filter',this.get('filter'));
  },

  actions:{

   openTableMetaInfo(id) {
      var allTableList = this.get('controller').get('filteredHeatTables');
      var sourceTableId = id;

      let selectedTable = allTableList.filterBy('tableId', parseInt(sourceTableId));
      this.get('controller').set('selectedTableForTableMeta', selectedTable[0]);
      this.send('openSliderTableMetaInfo');
   },

   testHook(id) {
      //console.log('testHook', id);
   },

   openSliderTableMetaInfo() {
      this.get('controller').set('showHeatmapDetails', true);
   },

   closeSliderTableMetaInfo() {
      this.get('controller').set('selectedTableForTableMeta', null);
      this.get('controller').set('showHeatmapDetails', false);
   },

   reloadHeatmap(){
      let dbId = this.get('selectedDb.id');
      let dbName = this.get('selectedDb.name');
      let duration = this.get('selectedDuration');

      let range = this.get('reportDateRange').getDateRange(duration);
      let endDate = range.endDate.format('YYYY-MM-DD');
      let startDate = range.startDate.format('YYYY-MM-DD');

      let groupBy = this.get('selectedDuration.groupBy');

      this.get( 'reportsCountDatabase').fetchReport(dbId , {startDate: startDate, endDate: endDate, groupedBy: groupBy }).then((data) => {
        this.get('controller').set('heatTables', tableHeatmap(data));
        this.get('controller').set('filteredHeatTables', this.get('controller').get('heatTables') );
        this.get('controller').set('isLoading', false);
        this.get('controller').set('showHeatmapDetails', false);
        if(this.get('selectedDuration.id')) {
          this.transitionTo('reports.database.database-read.relations', this.get('selectedDb.name'), this.get('controller').get('filteredHeatTables').get('firstObject.tableId'), { queryParams: { filter: this.get('selectedDuration.id') }});
        }
      });
    },

    handleDbChange(selection, list){
      console.log('You have changed the DB.');
      this.get('controller').set('isLoading', true);
      let selectedDb = {id: selection.id, name: selection.name};
      this.set('selectedDb', selectedDb);
      this.get('controller').set('selectedDb',this.get('selectedDb'));
      this.send('reloadHeatmap');
    },

    handleDurationChange (selection, list){
      this.get('controller').set('isLoading', true);
      console.log('You have changed the duration.');
      let selectedDuration = selection;
      this.set('selectedDuration', selectedDuration);
      this.get('controller').set('selectedDuration', this.get('selectedDuration'));
      this.send('reloadHeatmap');
    },

    filterHeatTable(text){
      this.get('controller').set('isLoading', true);
      console.log('Filter HeatTables for text:: ', text);
      let filterHeatTables = this.get('controller').get('heatTables').filter((item)=>{
        if(item.tableName.toLowerCase().includes(text.toLowerCase())){
          return item;
        }
      });

      this.get('controller').set('filteredHeatTables', filterHeatTables );
      this.get('controller').set('isLoading', false);
      this.get('controller').set('showHeatmapDetails', false);
      this.transitionTo('reports.database.database-read.relations', this.get('controller').get('filteredHeatTables').get('firstObject.tableId'), { queryParams: { filter: this.get('selectedDuration.id') }});
    }
  }

});
