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
import databaseReadsMocks from '../../../../configs/database-reads-mocks';
import tableHeatmap from '../../../../utils/table-heatmap';
import tableRelations from '../../../../utils/table-relations';
import primaryDuration from '../../../../configs/report-range-primary';
import secondaryDuration from '../../../../configs/report-range-secondary';
import countTypes from '../../../../configs/count-types';

export default Ember.Route.extend({

  breadCrumb: {
   'title': 'Relations',
   'linkable': false
  },

  ajax: Ember.inject.service(),

  selectedTableId: null,
  selectedDb: null,

  primaryDuration: primaryDuration,
  secondaryDuration: secondaryDuration,

  countTypes: countTypes,

  selectedCountType: Ember.computed('countTypes', function(){
    return this.get('countTypes').filterBy('selected', true).get('firstObject');
  }),

  reportsCountDatabase: Ember.inject.service('reports-count-database'),
  allDurations: Ember.computed('primaryDuration','secondaryDuration', function(){
    return this.get('primaryDuration').concat(this.get('secondaryDuration'));
  }),

  selectedDuration: Ember.computed('allDurations', function(){
    return this.get('allDurations').filterBy('default', true).get('firstObject');
  }),

  reportDateRange: Ember.inject.service('report-date-range'),

  model(params){

    let duration = this.controllerFor('reports.database.database-read').get('selectedDuration') || this.get('selectedDuration');
    let range = this.get('reportDateRange').getDateRange(duration);
    let endDate = range.endDate.format('YYYY-MM-DD');
    let startDate = range.startDate.format('YYYY-MM-DD');

    let groupBy = duration.groupBy;
    let selectedTableId = params.table_id;

    this.set('selectedTableId', selectedTableId);
    this.set('selectedDb', params.database);
    let realtions = this.get( 'reportsCountDatabase').fetchTableRelations(selectedTableId , {startDate: startDate, endDate: endDate, groupedBy: groupBy }).catch((error) => {
      console.error(error);
    });
    return realtions;
  },

  setupController(controller, model) {
    this._super(...arguments);

    controller.set('isLoading', true);

    var self = this;

    if(!model){
      return;
    }
    if(!model.joins) {
      return;
    }

    var allConnections = tableRelations(model.joins);

    let xman = controller.get('selectedCountType') ? controller.get('selectedCountType').get('id') : 'projectionCount';
    controller.set('heatTables', tableHeatmap(model, xman ));

    let tableNamesHash = {};
    let columnsNamesHash = {};

    controller.get('heatTables').forEach( table =>{
      tableNamesHash[table.tableId] = table.tableName;
      table.columns.forEach( column =>{
        columnsNamesHash[column.id] = column.name;
      })
    })

    controller.set('tableNamesHash', tableNamesHash);
    controller.set('columnsNamesHash', columnsNamesHash);

    controller.set('countTypes', this.get('countTypes'));
    controller.set('selectedCountType', this.get('selectedCountType'));    

    allConnections.forEach(connection =>{

      let connDetails = connection.link.split('-');
      let leftTableId = parseInt(connDetails[0]);
      let rightTableId = parseInt(connDetails[1]);

      var updatedHeatTables = controller.get('heatTables').map(table =>{
        if(table.tableId == leftTableId){

          if(!!table.linkedTo){
            table["linkedTo"].push({node: parseInt(rightTableId), connections: connection.connections});
          } else{
            table["linkedTo"] = [];
            table["linkedTo"].push({node: parseInt(rightTableId), connections: connection.connections});
          }

        } else if(table.tableId == rightTableId){

          if(!!table.linkedTo){
            table["linkedTo"].push({node: parseInt(leftTableId), connections: connection.connections});
          } else{
            table["linkedTo"] = [];
            table["linkedTo"].push({node: parseInt(leftTableId), connections: connection.connections});
          }
        }

        return table;
      });

      controller.set('allTableList', updatedHeatTables);
    });

    controller.set('allTableList', controller.get('allTableList') || controller.get('heatTables'));

    let selectedCountType = controller.get('selectedCountType').get('id') || 'projectionCount';

    let localSelectedTableArr = controller.get('allTableList').filter((table)=>{ 
      if(table.tableId == self.get('selectedTableId')){
        let sortedTableColumns = table.columns.sort(function (a, b) { return b[selectedCountType]  - a[selectedCountType] });
        table.columns = sortedTableColumns;
        return table;
      }
    })

    controller.set('selectedTableModel', localSelectedTableArr[0] || controller.get('heatTables')[0]);

    this.getMetricsScaleHash(controller.get('allTableList'), controller.get('selectedCountType'));
 
    controller.set('noReadCount', !controller.get('selectedTableModel').columns.length ? true : false );
    controller.set('selectedTableForTableMeta', model.joins);
    controller.set('isLoading', false);
  },

  countForCountType(expr, column, callback){
    return callback(expr, column) 
  },

  parseColumByCountType(countType, column){
    return parseInt(column[countType]);
  },

  getMetricsScaleHash(allTableList, selectedCountType){
    let expr = selectedCountType.get('id');
    let max, min;
    allTableList.forEach((table) => {
      table.columns.forEach((column) => {
        if (max == undefined) {
          max = this.countForCountType(expr, column, this.parseColumByCountType )
        } else {
          if (parseInt(column[expr]) > max) {
            max = this.countForCountType(expr, column, this.parseColumByCountType )
          }
        }

        if (min == undefined) {
          min = this.countForCountType(expr, column, this.parseColumByCountType )
        } else {
          if (parseInt(column[expr]) < min) {
            min = this.countForCountType(expr, column, this.parseColumByCountType )
          }
        }
      })
    });

    let delta = parseInt((max - min) / 4);

    if((max - min) > 10){
      var metricsScaleOneLow = min;
      var metricsScaleOneHigh = min + delta * 1;
      var metricsScaleTwoLow = min + delta * 1 + 1;
      var metricsScaleTwoHigh = min + delta * 2;
      var metricsScaleThreeLow = min + delta * 2 + 1;
      var metricsScaleThreeHigh = min + delta * 3;
      var metricsScaleFourLow = min + delta * 3 + 1;
      var metricsScaleFourHigh = max;
    } else {
      var metricsScaleOneLow = 0;
      var metricsScaleOneHigh = 1;
      var metricsScaleTwoLow = 2;
      var metricsScaleTwoHigh = 3;
      var metricsScaleThreeLow = 4;
      var metricsScaleThreeHigh = 5;
      var metricsScaleFourLow = 6;
      var metricsScaleFourHigh = 10;
    }

    let metricsScaleHash = {
      metricsScaleOneLow: metricsScaleOneLow,
      metricsScaleOneHigh: metricsScaleOneHigh,
      metricsScaleTwoLow: metricsScaleTwoLow,
      metricsScaleTwoHigh: metricsScaleTwoHigh,
      metricsScaleThreeLow: metricsScaleThreeLow,
      metricsScaleThreeHigh: metricsScaleThreeHigh,
      metricsScaleFourLow: metricsScaleFourLow,
      metricsScaleFourHigh: metricsScaleFourHigh
    };

    this.get('controller').set('metricsScaleHash', metricsScaleHash);

  },

  actions:{

    showPathDetails(pathJson){
      let tableNamesHash = this.get('controller').get('tableNamesHash');
      let columnsNamesHash = this.get('controller').get('columnsNamesHash');

      let updatedPathJson = Ember.A();

      pathJson.forEach( path => {
        let updatedPath = Object.assign({}, path);
        updatedPath["leftTableName"] = tableNamesHash[path.leftTableId];
        updatedPath["rightTableName"] = tableNamesHash[path.rightTableId];
        updatedPath["leftColumnName"] = columnsNamesHash[path.leftColumnId];
        updatedPath["rightColumnName"] = columnsNamesHash[path.rightColumnId];

        updatedPathJson.push(updatedPath);
      })

      this.get('controller').set('pathJson', updatedPathJson);
      this.send('openPathSlider');
    },

    closePathSlider(){
      this.get('controller').set('showPathDetails', false);
    },

    openPathSlider(){
      this.get('controller').set('showPathDetails', true);
      this.send('closeSliderTableMetaInfo');
    },

    updateCountTypes(type){
      let countTypes = this.get('countTypes');
      let updatedCountTypes = countTypes.map((item)=>{
        if(item.get('id') == type){
          item.set('selected', true);
        } else {
          item.set('selected', false);
        }
        return item;
      })

      this.get('controller').set('countTypes', updatedCountTypes);

      let selectedCountType = updatedCountTypes.filterBy('selected', true).get('firstObject');
      this.get('controller').set('selectedCountType', selectedCountType);
      this.send('sortTablesByCountType');
      this.getMetricsScaleHash(this.get('controller').get('allTableList'), this.get('controller').get('selectedCountType'));
    },

    sortTablesByCountType(){
      let selectedCountType = this.get('controller').get('selectedCountType');
      let allTableList = this.get('controller').get('allTableList').map(item =>{
        let { bytesRead, bytesWritten, columnsInfo, connections, linkedTo, metaInfo, readCount, recordsRead, recordsWritten, tableId, tableName, type, writeCount, columns } = item;
        let obj = {bytesRead, bytesWritten, columnsInfo, connections, linkedTo, metaInfo, readCount, recordsRead, recordsWritten, tableId, tableName, type, writeCount, columns:columns.sort( (a, b) => { return b[selectedCountType.get('id')]  - a[selectedCountType.get('id')] })};   
        return obj;
      });
    }
  }
});
