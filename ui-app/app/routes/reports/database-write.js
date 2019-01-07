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
import databaseWritesMocks from '../../configs/database-writes-mocks';
import tableHeatmap from '../../utils/table-heatmap';

export default Ember.Route.extend({

  alldatabases: null,
  selectedDb: {id: 1, name: "default"},

  reportsCountDatabase: Ember.inject.service('reports-count-database'),

  allDurations: [{name: 'Last Week', groupBy: 'DAILY', countBy: 'days', offsetBy: 7},
                 {name: 'Last Month', groupBy: 'WEEKLY' , countBy: 'days', offsetBy: 30},
                 {name: 'Last Six Month', groupBy: 'MONTHLY', countBy: 'months', offsetBy: 6},
                 {name: 'Last Year', groupBy: 'QUARTERLY', countBy: 'months', offsetBy: 12},
                 {name: 'Today', groupBy: 'DAILY', countBy: 'days', offsetBy: 1}],

  selectedDuration: {name: 'Last Week', groupBy: 'DAILY', countBy: 'days', offsetBy: 7},

  model(){
    //return databaseWritesMocks;

    let dbId = this.get('selectedDb.id');
    let dbName = this.get('selectedDb.name');
    let duration = this.get('selectedDuration');

    let groupBy = this.get('selectedDuration.groupBy');
    let endDate = moment().format('YYYY-MM-DD');
    let startDate = moment().subtract(duration.countBy, duration.offsetBy).format('YYYY-MM-DD');


    var tableReports = this.store.adapterFor('reports-count-database').fetchReport(dbId , {startDate: startDate, endDate: endDate, groupedBy: groupBy });
    return tableReports;

    //check why below code is not working.
    /*
    this.get( 'reportsCountDatabase').fetchReport(dbId , {startDate: startDate, endDate: endDate, groupedBy: groupBy }).then(data =>{
      console.log('I am in reportsCountDatabase callback in route');
      return data;
    });
    */

  },

  setupController(controller, model) {
    this._super(...arguments);

    controller.set('allDurations',this.get('allDurations'));
    controller.set('selectedDuration',this.get('selectedDuration'));

    controller.set('model', model);
    controller.set('tables', model.tables);
    controller.set('reports', model.reports);
    controller.set('heatTables', tableHeatmap(model));

    //Call for all avaiable databases for search
    this.store.findAll('database').then((data) => {
        let dbArray = [];
        data.forEach((item, index) => {
          let tempDb = {};
          tempDb['id'] = index+1;
          tempDb['name'] = item.get('name');

          dbArray.push(tempDb);
        });
        this.set('alldatabases', dbArray);
    })
    .then(() => {
        controller.set('alldatabases',this.get('alldatabases'));
        controller.set('selectedDb',this.get('selectedDb'));
    });

  },

  actions:{

    reloadHeatmap(){
      let dbId = this.get('selectedDb.id');
      let dbName = this.get('selectedDb.name');
      let duration = this.get('selectedDuration');

      let groupBy = this.get('selectedDuration.groupBy');
      let endDate = moment().format('YYYY-MM-DD');
      let startDate = moment().subtract(duration.countBy, duration.offsetBy).format('YYYY-MM-DD');

      this.store.adapterFor('reports-count-database').fetchReport(dbId , {startDate: startDate, endDate: endDate, groupedBy: groupBy }).then((data) => {
       this.get('controller').set('heatTables', tableHeatmap(data));
      })
    },

    handleDbChange(selection, list){
      console.log('You have changed the DB.');
      let selectedDb = {id: selection.id, name: selection.name};
      this.set('selectedDb', selectedDb);
      this.get('controller').set('selectedDb',this.get('selectedDb'));
      this.send('reloadHeatmap');
    },

    handleDurationChange (selection, list){
      console.log('You have changed the duration.');
      let selectedDuration = selection;
      this.set('selectedDuration', selectedDuration);
      this.get('controller').set('selectedDuration', this.get('selectedDuration'));
      this.send('reloadHeatmap');
    }

  }

});
