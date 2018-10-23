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

export default Ember.Service.extend({

  store: Ember.inject.service(),
  isColumnPromiseCancelled: false,
  dataBaseTableColumns:{},
  tableMetaDataList: {},
  autoRefreshTime: 25000,
  enableBrowserStorage: false,
  createJob(payload){
    return new Ember.RSVP.Promise( (resolve, reject) => {
      this.get('store').adapterFor('query').createJob(payload).then(function(data) {
        resolve(data);
      }, function(err) {
        reject(err);
      });
    });
  },
  getJob(jobId, firstCall){
    return new Ember.RSVP.Promise( (resolve, reject) => {
      this.get('store').adapterFor('query').getJob(jobId, firstCall).then(function(data) {
        resolve(data);
      }, function(err) {
          reject(err);
      });
    });
  },

  saveToHDFS(jobId, path){
    return this.get('store').adapterFor('job').saveToHDFS(jobId, path);
  },

  downloadAsCsv(jobId, path){
    return this.get('store').adapterFor('job').downloadAsCsv(jobId, path);
  },

  retrieveQueryLog(jobId){
    return new Ember.RSVP.Promise( (resolve, reject) => {
      this.get('store').adapterFor('query').retrieveQueryLog(jobId).then(function(data) {
        resolve(data);
      }, function(err) {
        reject(err);
      });
    });
  },

  getVisualExplainJson(jobId){
    return new Ember.RSVP.Promise( (resolve, reject) => {
      this.get('store').adapterFor('query').getVisualExplainJson(jobId).then(function(data) {
          resolve(data);
        }, function(err) {
          reject(err);
        });
    });
  },

  getTableInfo(db, name, cacheResults){
    let rec = this.get('store').peekRecord('table-info', db+"/"+name);
    if(rec && cacheResults) {
        return new Ember.RSVP.Promise( (resolve, reject) => {
              resolve(rec);
        });
    }
    return new Ember.RSVP.Promise( (resolve, reject) => {
        this.get("store").queryRecord('tableInfo', { databaseId: db, tableName: name}).then(function(data) {
          resolve(data);
        }, function(err) {
          reject(err);
        });
    });
  },

  refreshTableData(setDatabaseDetails, setTableDetails, interval, filteredItems, ColumnDefinition) {
    if(this.checkIfDataExistsAlready(filteredItems)) {
      this.synchTableMetaDataToComp(filteredItems, setTableDetails);
    } else {
      this.fetchDatabaseAndTableDetails(filteredItems, ColumnDefinition, setTableDetails, setDatabaseDetails);
    }
    Ember.run.later(() => {
      if(this.get("autorefresh")) {
        this.fetchDatabaseAndTableDetails(filteredItems, ColumnDefinition, setTableDetails, setDatabaseDetails);
      }
    }, this.get('autoRefreshTime'));
  },

  fetchDatabaseAndTableDetails(filteredItems, ColumnDefinition, setTableDetails, setDatabaseDetails) {
    this.fetchDatabaseDetails(setDatabaseDetails);
    this.synchTableMetaDataToComp(filteredItems, setTableDetails);
    this.getDataBaseAndTableInfo(filteredItems, ColumnDefinition, this.synchTableMetaDataToComp.bind(this), setTableDetails);
  },

  fetchDatabaseDetails(callback) {
    this.get("store").findAll('database').then((data) => {
      callback(data);
    });
  },
  checkIfDataExistsAlready(filteredItems) {
    return this.get(`tableMetaDataList.${filteredItems[0].dbName}`) || this.get(`tableMetaDataList.${filteredItems[0].dbname}`);
  },
  synchTableMetaDataToComp(filteredItems, callback) {
    const data = this.checkIfDataExistsAlready(filteredItems);
    if(data) {
      callback(data.tableMetadata);
    }
  },

  setTableMetaDataAfterFetch(tableMetadata) {
    this.set("tableMetaData", tableMetadata);
    this.set(`tableMetaDataList.${tableMetadata.dbname}`, {dbname:tableMetadata.dbname, tableMetadata:tableMetadata});
  },

  getDataBaseAndTableInfo(filteredItems, ColumnDefinition, synchTableMetaDataToComp, setTableDetails) {
    var self = this, tableNames = [], deferred = Ember.RSVP.defer();
    var counter = 0;
    filteredItems.forEach(function(item, index){
      let db = item.dbName?item.dbName:item.dbname;
      self.get('store').adapterFor('table').fetchAllTables(item.id, db).then(function(tableMetaData){
        var tableDetails = [], tableData = tableMetaData.databaseWithTableMeta.tableMetas;
        var tableCount = tableData.get("length");
        if(tableCount === 0) {
          self.setTableMetaDataAfterFetch({facetedFields: [], dbname:db});
          synchTableMetaDataToComp(filteredItems, setTableDetails);
        }
        tableData.forEach(function(data, j) {
          let dataOfTable = data;
          var facetsArr = [], columns = [], tableName = data.table;
          counter++;
          dataOfTable.columns.forEach(function(dataItem, ind) {
            facetsArr.push({"value": dataItem.name});
            columns.push(dataItem.name);
          });
          tableDetails.push({"name":tableName, columns:facetsArr});
          tableNames.push({'column': ColumnDefinition.make({id: tableName, headerTitle: tableName}), name: tableName, facets: facetsArr, columns:columns});
          self.set('tableNames', {facetedFields: tableNames});
          if(counter === tableCount) {
            let extractedData = {"dbname":db, tables:tableNames};
            if(self.get('enableBrowserStorage')) {
              self.saveColumnData(db, extractedData);
            } else {
              self.set(`dataBaseTableColumns.${db}`, extractedData);
            }
            self.setTableMetaDataAfterFetch({facetedFields: tableNames, dbname:db});
            synchTableMetaDataToComp(filteredItems, setTableDetails);
          }
        });
      });
    });
  },
  saveColumnData(db, extractedData) {
    localStorage.setItem(`dataBaseTableColumns.${db}`, JSON.stringify(extractedData));
  },
  extractColumnData(db){
    if(this.get('enableBrowserStorage')) {
      return [JSON.parse(localStorage.getItem(`dataBaseTableColumns.${db}`))];
    } else {
      return [this.get("dataBaseTableColumns")[db[0]]];
    }
  },
  extractTableNamesAndColumns(database){
    const dataBaseTableColumns = this.extractColumnData(database);
    return dataBaseTableColumns;
  },

  getRecommendations(queryId){
    return new Ember.RSVP.Promise( (resolve, reject) => {
      this.get('store').adapterFor('recommendations').getRecommendations(queryId).then(function(data) {
        resolve(data);
      }, function(err) {
        reject(err);
      });
    });
  },

  setColumnPromiseCancel() {
   this.set("isColumnPromiseCancelled", true);
  },

  startRefresh() {
    this.set("autorefresh", true);
  },

  stopRefresh() {
    this.set("autorefresh", false);
  }

});
