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
import ENV from '../../config/environment';
import tabs from '../../configs/result-tabs';
import UILoggerMixin from '../../mixins/ui-logger';
import commons from '../../mixins/commons';

export default Ember.Route.extend(UILoggerMixin, commons, {

  breadCrumb: null,

  query: Ember.inject.service(),
  jobs: Ember.inject.service(),
  savedQueries: Ember.inject.service(),
  autoRefreshReplInfo: Ember.inject.service(),
  isQueryEdidorPaneExpanded: false,
  isQueryResultPanelExpanded: false,
  globalSettings: '',
  tezViewInfo: Ember.inject.service(),
  hiveWorksheetId: null,

  beforeModel(params){
    let existingWorksheets = this.store.peekAll('worksheet');
    existingWorksheets.setEach('selected', false);
    this.closeAutocompleteSuggestion();
    this.setActiveTab('queries');
  },

  afterModel(model) {
    var self = this;
    let dbmodel = this.store.peekAll('database');
    if (!dbmodel.get("length")) {
        dbmodel = this.store.findAll('database').then((data) => {}, (reason) => {
                     model.set("apiAcessError", true);
                     this.setErrorPageONCompose();
                  });
    }

    this.store.findAll('setting').then((data) => {
      let localStr = '';
      data.forEach(x => {
        localStr = localStr + 'set '+ x.get('key')+ '='+ x.get('value') + ';\n';
      });
      this.set('globalSettings', localStr);
    });

    //lastResultRoute
    console.log('lastResultRoute:: ', model.get('lastResultRoute'));
    let lastResultRoute = model.get('lastResultRoute');

    if(Ember.isEmpty(lastResultRoute)){
      if(model.get('jobData').length > 0){
        this.transitionTo('queries.query.results');
      } else {
        this.transitionTo('queries.query');
      }
    } else {
      this.transitionTo('queries.query' + lastResultRoute);
    }
    return dbmodel;
  },
  model(params) {
    let selectedWs = this.store.peekAll('worksheet').filterBy('id', params.worksheetId.toLowerCase()).get('firstObject');
    if(selectedWs) {
      selectedWs.set('selected', true);
      return selectedWs;
    } else {
      this.transitionTo('queries');
    }
  },

  activate() {
    if(ENV.APP.SHOULD_AUTO_REFRESH_REPL_INFO) {
      this.get('autoRefreshReplInfo').startReplAutoRefresh(() => {
        console.log("Repl AutoRefresh started");
      }, this._replInfoRefreshed.bind(this), 0);
    }
  },

  _replInfoRefreshed(){
    let timeSinceLastUpdate = this.get('autoRefreshReplInfo').getTimeSinceLastUpdate();
    this.get('controller').set('timeSinceLastUpdate',  timeSinceLastUpdate);
  },

  deactivate() {
    this.get("query").stopRefresh();

    if(ENV.APP.SHOULD_AUTO_REFRESH_REPL_INFO) {
      this.get('autoRefreshReplInfo').stopReplAutoRefresh();
    }

  },
  setupController(controller, model) {

    this._super(...arguments);

    Ember.$(document).on('click', '.save-as-container .dropdown-menu', function (e) {
      e.stopPropagation();
    });

    Ember.$('.save-as-container').on('show.bs.collapse',  () => {
      let wf = this.store.peekAll('worksheet').filterBy('id', this.paramsFor('queries.query').worksheetId.toLowerCase()).get('firstObject');
      controller.set('worksheetTitle', wf.get('title'));
      Ember.$('#worksheet-title').val(wf.get('title'));
    });

    let self = this, selectedDb;
    let alldatabases = this.store.peekAll('database');
    controller.set('alldatabases',alldatabases);

    selectedDb = this.checkIfDeafultDatabaseExists(alldatabases);

    let { dbName, id } = selectedDb;
    let selectedTablesModels =[];
    let selectedMultiDb = [];
    if(dbName) {
      controller.set('model.selectedDb', dbName);
      controller.set('selectedDb', dbName);
      controller.set('model.selectedDbId', id);
      controller.set('selectedDbId', id);



      selectedTablesModels.pushObject({
        dbName,
        id,
        'tables': this.store.query('table', {databaseId: dbName}),
        'isSelected': true
      });
      selectedMultiDb.pushObject(dbName);
    }

    var fsRes = this.store.peekAll('file-resource');
     this.store.findAll('file-resource').then((data) => {
          let fileResourceList = [];
          data.forEach(x => {
            let localFileResource = {'id': x.get('id'),
              'name': x.get('name'),
              'path': x.get('path'),
              'owner': x.get('owner')
            };
            fileResourceList.push(localFileResource);
          });
          controller.set('fileResourceList', fileResourceList);
        });

        this.store.findAll('udf').then((data) => {
          let allUDFList = [], systemUDFList = [];
          data.forEach(x => {
            if(x.get('type') === 'system') {
              let localUDF = {'id': x.get('id'),
                'name': x.get('name'),
              };
              systemUDFList.push(localUDF);
            } else {
              let localUDF = {'id': x.get('id'),
                'name': x.get('name'),
                'classname': x.get('classname'),
                'fileResource': x.get('fileResource'),
                'owner': x.get('owner')
              };
              allUDFList.push(localUDF);
            }

          });
          controller.set('allUDFList', allUDFList);
          controller.set('systemUDFList', systemUDFList);
        });

    controller.set('worksheet', model);
    controller.set('selectedTablesModels',model.get('selectedTablesModels') || selectedTablesModels );
    controller.set('selectedMultiDb', model.get('selectedMultiDb') || selectedMultiDb);
    controller.set('isQueryRunning', model.get('isQueryRunning'));
    controller.set('currentQuery', model.get('query'));
    controller.set('currentJobId', model.get('currentJobId'));
    controller.set('queryResult', model.get('queryResult'));
    controller.set('isJobSuccess', model.get('isJobSuccess'));
    controller.set('isJobCancelled', model.get('isJobCancelled'));
    controller.set('isJobCreated', model.get('isJobCreated'));
    controller.set('isExportResultSuccessMessege', false);
    controller.set('isExportResultFailureMessege', false);
    controller.set('showSaveHdfsModal', false);
    controller.set('logResults', model.get('logResults') || '');
    controller.set('isVisualExplainQuery', false);
    controller.set('visualExplainJson', model.get('visualExplainJson'));
    controller.set('showWorksheetModal',false);
    controller.set('worksheetModalSuccess',false);
    controller.set('worksheetModalFail',false);
    if(controller.get('model.showResults') === false) {
      controller.set('model.showResults', false);
    } else {
      controller.set('model.showResults', true);
    }
    if(controller.get('model.downloadResults') !== true) {
      controller.set('model.downloadResults', false);
    }
    if(controller.get('isShowSavedquery') === undefined) {
      controller.set('isShowSavedquery', true);
      controller.set('isShowUDF', false);
    }
    if(controller.get('model.queryEditorResizerDisabled')){
       controller.set('model.queryEditorResizerDisabled', true);
    }
    controller.set("savedQuerylist", []);
    controller.set("model.savedQuerylist", []);

    if(model.get("id") === "saved") {
      this.store.findAll('savedQuery').then(savedQueries => {
        controller.set("savedQuerylist", savedQueries.toArray());
        controller.set("model.savedQuerylist", savedQueries.toArray());
      });
    }
    if(controller.get("model.query-status-class") === 'error') {
      this.showNotification(controller.get("model.query-status"), controller.get("model.query-status-class"), controller.get("model.error"));
    } else {
      controller.set('tabs', tabs);
    }
    controller.set('worksheetTitle', model.get('title'));
    controller.set('savedQueryErrorMsg', null);
    controller.set('disable_UDF_SETTINGS', ENV.APP.DISABLE_UDF_SETTINGS);
    controller.set('highlightedText', '')

  },

  checkIfDeafultDatabaseExists(alldatabases){
    let selectedDbClass;
    let selectedDbName = this.get('controller.model').get('selectedDb');
    if(!!selectedDbName){
      selectedDbClass = alldatabases.filterBy('name', selectedDbName ).get('firstObject');
    } else {
      selectedDbClass = alldatabases.filterBy('name', "default").get('firstObject'); 
    }

    if(!!selectedDbClass){
      return {dbName : selectedDbClass.get("name"), id: selectedDbClass.get("id")} ;
    } else{
      selectedDbClass = alldatabases.sortBy('id').get('firstObject');
      return {dbName : selectedDbClass.get("name"), id: selectedDbClass.get("id")} ; 
    }
  },

  setSelectedDB(selectedDBs) {
    let selectedDb = this.get('controller.model').get('selectedDb');
    if(selectedDBs && selectedDBs.indexOf(selectedDb) === -1) {
      this.get('controller.model').set('selectedDb', selectedDBs);
    }
    else if(selectedDBs.length === 0) {
      this.get('controller.model').set('selectedDb', null);
    }
  },
  closeWorksheetAfterSave(){
    let tabDataToClose = this.get('controller.model').get('tabDataToClose');
    if(tabDataToClose) {
      this.send('closeWorksheet', tabDataToClose.index, tabDataToClose.id);
    }
  },
  queryTextTobeExecuted() {
    let queryTobeExecuted, selectedQueryTextArr = this.get('controller').get('highlightedText') || [];
    let selectedQueryText = selectedQueryTextArr.join("")
    if(selectedQueryText.length) {
        return selectedQueryText;
    }
    queryTobeExecuted = this.get('controller').get('currentQuery');
    return queryTobeExecuted;
  },
  showNotification(message, type, data){
     this.get("controller").set("tabs", [  Ember.Object.create({
                                             name: 'error-log',
                                             text: 'Message',
                                             label: 'Message',
                                             link: 'queries.query.error-log',
                                             routeName: 'queries.query.error-log',
                                             faIcon: 'list'
                                           })]);
     this.get("controller").set("show-status", true);
     this.get("controller.model").set("show-status", true);
     this.get("controller").set("query-status", message);
     this.get("controller.model").set("query-status", message);
     this.get("controller").set("query-status-class", type);
     this.get("controller.model").set("query-status-class", type);
     this.get("controller").set("error", data);
     this.get("controller.model").set("error", data);
     this.transitionTo('queries.query.error-log');
  },
  hideNotification(){
     this.get("controller").set("show-status", false);
     this.get("controller.model").set("show-status", false);
  },
  checkIfVisualExplainExist(data) {
    var tabsMod = [tabs[0], tabs[1]];
    try {
        if(data.rows[0] && data.rows[0][0] && JSON.parse(data.rows[0][0])['STAGE PLANS']) {
          this.get("controller").set("tabs", tabs);
        } else {
          this.get("controller").set("tabs", tabsMod);
        }
    } catch(error){
        this.get("controller").set("tabs", tabsMod);
    }
  },

  validateSavedQuery(title){


    let originalQuery = this.queryTextTobeExecuted();
    if(Ember.isBlank(originalQuery)) {
      this.get("controller").set("savedQueryErrorMsg", "Query can not be empty.");
      return true;
    } else if(!/^[a-zA-Z0-9-_ ]+$/.test(title)) {
      this.get("controller").set("savedQueryErrorMsg", "Special characters or spaces are not allowed.");
      return true;
    } else{
      return false;
    }
  },
  actions: {
    showPreview(tableName) {

     this.get('controller').set('dataPreview', true);
     let existingWorksheets = this.store.peekAll('worksheet'), self = this;
     let newWorksheetName = 'worksheet';
           self.transitionTo('queries.query.loading');

     if(!this.controllerFor("queries").worksheetCount && !existingWorksheets.get("length")) {
       newWorksheetName = newWorksheetName + 1;
     } else {
       let id = parseInt(this.controllerFor("queries").worksheetCount);
       if(!id){
         id = existingWorksheets.get("length")+1;
       }
       newWorksheetName = newWorksheetName + id;
     }
     let queryString = 'select * from '+tableName+' limit 20;';
     let newWorksheetTitle = newWorksheetName.capitalize();
     this.store.createRecord('worksheet', {
       id: newWorksheetName,
       title: newWorksheetTitle,
       isQueryDirty: false,
       query: queryString,
       //owner: 'admin',
       selected: true
     });
     existingWorksheets.setEach('selected', false);
     this.controllerFor('queries').set('worksheets', this.store.peekAll('worksheet'));
     this.transitionTo('queries.query', newWorksheetTitle);

     let ctrlr = self.get('controller'), ctrlrModel = self.get('controller.model');
      this.get('controller.model').set('currentJobId', null);
      this.get('controller').set('currentJobId', null);


      let originalQuery = this.get('controller').get('currentQuery');

      let queryInput = originalQuery;


      self.send('expandQueryResultPanel');

      let dbid = this.get('controller.model').get('selectedDb');
      let worksheetTitle = this.get('controller.model').get('title');

      self.get('controller.model').set('queryResult', {'schema' :[], 'rows' :[]});


      let globalSettings = this.get('globalSettings');

      let queryStr = globalSettings + queryString;
      this.send('showQueryResultContainer');

      let payload = {
        "title":newWorksheetTitle,
        "selectedDatabase":dbid,
        "query":queryStr,
        "referrer":"job",
        "globalSettings":globalSettings};


      this.get('query').createJob(payload).then(function(data) {
        self.get('controller.model').set('currentJobData', data);
        self.get('controller.model').set('currentJobId', data.job.id);
        self.get('controller').set('dataPreview', false);

        self.get('jobs').waitForJobToComplete(data.job.id, 2 * 1000, false)
          .then((status) => {
            let jobDetails = self.store.peekRecord('job', data.job.id);
            self.send('getJobResult', data, payload.title, jobDetails, ctrlrModel);
            self.showNotification('Query has been submitted.', 'success');


          }, (error) => {
            console.log('error', error);
            self.showNotification('Failed to execute query.', /*self.extractError(error)*/ 'error', self.extractError(error));
            self.send('resetDefaultWorksheet', ctrlrModel);
          });

      }, function(error) {
        console.log(error);
        self.showNotification('Failed to execute query.', /*self.extractError(error)*/ 'error', self.extractError(error));
        //self.get('logger').danger('Failed to execute query.', self.extractError(error));
        self.send('resetDefaultWorksheet', ctrlrModel);
      });



    },
    resetDefaultWorksheet(currModel){
      if(!currModel) {
        currModel = this.get('controller.model');
      }
      currModel.set('queryResult',{'schema' :[], 'rows' :[]});
      currModel.set('currentPage',0);
      currModel.set('previousPage',-1);
      currModel.set('nextPage',1);
      //this.get('controller.model').set('selected',false);
      currModel.set('jobData',[]);
      currModel.set('currentJobData',null);
      currModel.set('query',"");
      currModel.set('logFile',"");
      currModel.set('logResults',"");
      currModel.set('isQueryRunning',false);
      currModel.set('isQueryResultContainer',false);
    },

    changeDbHandler(selectedDBs){
      let self = this;
      let selectedTablesModels =[];
      let selectedMultiDb = [];
      this.setSelectedDB(selectedDBs);
      selectedDBs.forEach((db, index) => {
        this.setSelectedDB(db.dbname);
        selectedTablesModels.pushObject(
          {
            dbname:db.dbname,
            id:db.id,
            'tables':self.store.query('table', {databaseId: db.dbname}),
            'isSelected': (index === 0) ? true :false
          }
        );
        selectedMultiDb.pushObject(db.dbname);
      });

      this.get('controller').set('selectedTablesModels', selectedTablesModels );
      this.get('controller.model').set('selectedTablesModels', selectedTablesModels );

      this.get('controller').set('selectedMultiDb', selectedMultiDb );
      this.get('controller.model').set('selectedMultiDb', selectedMultiDb );
    },

    showQueryResultContainer(){
      this.get('controller.model').set('isQueryResultContainer', true);
    },

    showTables(db){
      Ember.$('#' + db).toggle();
      this.get('controller.model').set('selectedDb', db);
    },

    visualExplainQuery(){
      this.send('executeQuery', true);
    },

    updateQuery(query){
      this.get('controller.model').set('query', query);
      if(Ember.isBlank(query)){
        this.get('controller.model').set('isQueryDirty', false);
      } else if(this.get('controller').get('currentQuery').indexOf(query) !== 0){
        this.get('controller.model').set('isQueryDirty', true);
      }
      this.get('controller').set('currentQuery', query);
    },
    executeQuery(isVisualExplainQuery) {

      this.hideNotification();

      let self = this, ctrlr = self.get('controller'), ctrlrModel = self.get('controller.model');
      let originalQuery = this.queryTextTobeExecuted();
      if(originalQuery && originalQuery.trim().split(" ")[0].toLowerCase() === "explain formatted ") {
        isVisualExplainQuery = true;
      } else if(originalQuery && originalQuery.trim().split(" ")[0].toLowerCase() === "explain"){
        isVisualExplainQuery = true;
      } else if(originalQuery && originalQuery.trim().split(" ")[0].toLowerCase() === "describe") {
        this.get('controller').set('isDescribeCommand', true);
        this.get('controller.model').set('isDescribeCommand', true);
      } else {
        this.get('controller').set('isDescribeCommand', false);
        this.get('controller.model').set('isDescribeCommand', false);
      }
      ctrlrModel.set('queryEditorResizerDisabled', false);
      if(this.get('controller.model.downloadResults')){
          this.get('controller').set('downloadResultsInit', 0);
          this.get('controller.model').set('downloadResultsInit', 0);
      }
      this.get('controller').set('isJobSuccess', null);
      this.get('controller.model').set('isJobSuccess', null);
      this.get('controller.model').set('currentJobId', null);
      this.get('controller').set('currentJobId', null);

      if(!Ember.isEmpty(isVisualExplainQuery)){
        isVisualExplainQuery = true;
        this.get('controller').set('isVisualExplainQuery', true);
      } else {
        isVisualExplainQuery = false;
        this.get('controller').set('isVisualExplainQuery', false);
      }
      if(Ember.isBlank(originalQuery)) {
        this.showNotification("Query cannot be empty.", "error");
        this.send('resetDefaultWorksheet', ctrlrModel);
        return;
      }
      if(!(this.get('controller.model.showResults') || this.get('controller.model.downloadResults'))) {
        this.showNotification("Either Show Results or Download Results should be selected.", "error");
        this.send('resetDefaultWorksheet', ctrlrModel);
        return;
      }
      let queryInput = originalQuery;



      if (isVisualExplainQuery) {
        queryInput = "";
        let queries = this.queryTextTobeExecuted().split(";").filter(function (query) {
          if (query && query.trim()) {
            return true;
          }
        });

        if(queries && queries.length) {
         if(queries[0].split(" ")[0].toLowerCase() === "explain") {
           queries[0] = queries[0].toLowerCase().replace("explain", "");
         }
        }


        for (let i = 0; i < queries.length; i++) {
          if (i === queries.length - 1) {
            if(queries[i].toLowerCase().startsWith("explain formatted ")){
              queryInput += queries[i] + ";";
            } else{
              queryInput += "explain formatted " + queries[i] + ";";
            }
          } else {
            queryInput += queries[i] + ";";
          }
        }
      }

      this.get('controller.model').set('query', this.get('controller').get('currentQuery'));


      let dbid = this.get('controller.model').get('selectedDb');
      let worksheetTitle = this.get('controller.model').get('title');

      this.get('controller.model').set('jobData', []);
      self.get('controller.model').set('currentPage', 0);
      self.get('controller.model').set('previousPage', -1 );
      self.get('controller.model').set('nextPage', 1);
      self.get('controller.model').set('queryResult', {'schema' :[], 'rows' :[]});
      self.get('controller.model').set('visualExplainJson', null);


      this.get('controller.model').set('isQueryRunning', true);
      ctrlrModel.set('isJobCreated',false);
      ctrlr.set('isJobCreated',false);

      //this.get('controller').set('queryResult', self.get('controller').get('queryResult'));
      //this.get('controller.model').set('queryResult', self.get('controller').get('queryResult'));

      let globalSettings = this.get('globalSettings');

      let queryStr = globalSettings + queryInput;
      this.send('showQueryResultContainer');

      let payload ={
        "title":worksheetTitle,
        "selectedDatabase":dbid,
        "query":queryStr,
        "referrer":"job",
        "globalSettings":globalSettings};


      this.get('query').createJob(payload).then(function(data) {
        self.get('controller.model').set('currentJobData', data);
        self.get('controller.model').set('logFile', data.job.logFile);
        self.get('controller').set('currentJobId', data.job.id);
        self.get('controller.model').set('currentJobId', data.job.id);
        ctrlrModel.set('isJobCreated',true);
        ctrlr.set('isJobCreated',true);
        ctrlrModel.set('isQueryRunning',false);
        ctrlr.set('isQueryRunning',false);

        self.get('jobs').waitForJobToComplete(data.job.id, 2 * 1000, false)
          .then((status) => {
            ctrlrModel.set('isJobSuccess', true);
            ctrlrModel.set('isJobCancelled', false);
            ctrlrModel.set('isJobCreated', false);
            ctrlr.set('isJobSuccess', true);
            ctrlr.set('isJobCancelled', false);
            ctrlr.set('isJobCreated', false);
            let jobDetails = self.store.peekRecord('job', data.job.id);
            self.send('getJobResult', data, payload.title, jobDetails, ctrlrModel);
            self.showNotification('Query has been submitted.', 'success');
            //self.send('expandQueryResultPanel');

          }, (error) => {

            console.log('error', error);
            ctrlrModel.set('isJobSuccess', false);
            ctrlrModel.set('isJobCancelled', false);
            ctrlrModel.set('isJobCreated', false);
            ctrlr.set('isJobSuccess', false);
            ctrlr.set('isJobCancelled', false);
            ctrlr.set('isJobCreated', false);
            self.showNotification('Failed to execute query.', 'error', self.extractError(error));

            //self.get('logger').danger('Failed to execute query.', self.extractError(error));
            self.send('resetDefaultWorksheet', ctrlrModel);
            self.get('controller.model').set('query', self.get('controller').get('currentQuery'));
          });

      }, function(error) {
        console.log(error);
        self.showNotification('Failed to execute query.', 'error', self.extractError(error));

        //self.get('logger').danger('Failed to execute query.', self.extractError(error));
        self.send('resetDefaultWorksheet', ctrlrModel);
        self.get('controller.model').set('query', self.get('controller').get('currentQuery'));
      });
    },

    stopQuery(){
      Ember.run.later(() => {
         let jobId = this.get('controller').get('currentJobId'), self = this, ctrlr = self.get('controller'), ctrlrModel = self.get('controller.model');
         this.get('jobs').stopJob(jobId)
           .then( data => {
              this.get('controller').set('isJobCancelled', true);
           }).catch(function (response) {
              self.get('controller').set('isJobCancelled', true);
           });
      }, 1000);
    },

    getJobResult(data, payloadTitle, jobDetails, ctrlrModel){
      let self = this;

      let isVisualExplainQuery = this.get('controller').get('isVisualExplainQuery');

      let jobId = data.job.id;

      let currentPage = this.get('controller.model').get('currentPage');
      let previousPage = this.get('controller.model').get('previousPage');
      let nextPage = this.get('controller.model').get('nextPage');

      this.get('query').getJob(jobId, true).then(function(data) {
        self.hideNotification();

        let existingWorksheets = self.get('store').peekAll('worksheet');
        let myWs = null;
        if(existingWorksheets.get('length') > 0) {
          myWs = existingWorksheets.filterBy('id', ctrlrModel.get('id').toLowerCase()).get('firstObject');
        }
        myWs.set('queryResult', data);

        self.checkIfVisualExplainExist(data);

        myWs.set('isQueryRunning', false);
        myWs.set('hasNext', data.hasNext);

        let localArr = myWs.get("jobData");
        localArr.push(data);
        myWs.set('jobData', localArr);
        myWs.set('currentPage', currentPage+1);
        myWs.set('previousPage', previousPage + 1 );
        myWs.set('nextPage', nextPage + 1);
        //self.get("controller").set("tabs", tabs);

        if(isVisualExplainQuery) {
          self.send('showVisualExplain', payloadTitle);
        } else {
          self.get('controller.model').set('visualExplainJson', null);

          if( self.paramsFor('queries.query').worksheetId && (self.paramsFor('queries.query').worksheetId.toLowerCase() === ctrlrModel.get('id').toLowerCase())){
            self.transitionTo('queries.query.loading');

            Ember.run.later(() => {
              self.transitionTo('queries.query.results');
            }, 1 * 100);
          }
        }
      }, function(error) {
        console.log('error' , error);
        self.showNotification('Failed to execute query.', 'error', self.extractError(error));
        //self.get('logger').danger('Failed to execute query.', self.extractError(error));
        self.send('resetDefaultWorksheet', ctrlrModel);
      });
    },

    showVisualExplain(payloadTitle){
       if( this.paramsFor('queries.query').worksheetId && this.paramsFor('queries.query').worksheetId.toLowerCase() === payloadTitle.toLowerCase()){
         this.transitionTo('queries.query.loading');
         Ember.run.later(() => {
           this.transitionTo('queries.query.visual-explain');
         }, 1);
       }
    },

    goNextPage(payloadTitle){

      let currentPage = this.get('controller.model').get('currentPage');
      let previousPage = this.get('controller.model').get('previousPage');
      let nextPage = this.get('controller.model').get('nextPage');
      let totalPages = this.get('controller.model').get("jobData").length;

      if(nextPage > totalPages){ //Pages from server
        var self = this;
        var data = this.get('controller.model').get('currentJobData');
        let jobId = data.job.id;

        this.get('query').getJob(jobId, false).then(function(data) {
          self.get('controller.model').set('queryResult', data);
          self.get('controller.model').set('isQueryRunning', false);
          self.get('controller.model').set('hasNext', data.hasNext);
          self.get('controller.model').set('hasPrevious', true);

          let localArr = self.get('controller.model').get("jobData");
          localArr.push(data);

          self.get('controller.model').set('jobData', localArr);
          self.get('controller.model').set('currentPage', currentPage+1);
          self.get('controller.model').set('previousPage', previousPage + 1 );
          self.get('controller.model').set('nextPage', nextPage + 1);
        }, function(error) {
            console.log('error' , error);
        });
      } else {
        //Pages from cache object
        this.get('controller.model').set('currentPage', currentPage+1);
        this.get('controller.model').set('previousPage', previousPage + 1 );
        this.get('controller.model').set('nextPage', nextPage + 1);
        this.get('controller.model').set('hasNext', this.get('controller.model').get('jobData')[this.get('controller.model').get('currentPage')-1].hasNext);
        this.get('controller.model').set('hasPrevious', (this.get('controller.model').get('currentPage') > 1) ? true : false );
        this.get('controller.model').set('queryResult', this.get('controller.model').get("jobData")[this.get('controller.model').get('currentPage')-1] );
      }

      let existingWorksheets = this.get('store').peekAll('worksheet');
      let myWs = null;
      if(existingWorksheets.get('length') > 0) {
        myWs = existingWorksheets.filterBy('id', payloadTitle.toLowerCase()).get('firstObject');
      }

      this.transitionTo('queries.query.loading');

      Ember.run.later(() => {
        this.transitionTo('queries.query.results', myWs);
      }, 1 * 1000);
    },

    goPrevPage(payloadTitle){
      let currentPage = this.get('controller.model').get('currentPage');
      let previousPage = this.get('controller.model').get('previousPage');
      let nextPage = this.get('controller.model').get('nextPage');
      let totalPages = this.get('controller.model').get("jobData").length;

      if(previousPage > 0){
        this.get('controller.model').set('currentPage', currentPage-1 );
        this.get('controller.model').set('previousPage', previousPage - 1 );
        this.get('controller.model').set('nextPage', nextPage-1);
        this.get('controller').set('queryResult', this.get('controller.model').get("jobData")[this.get('controller.model').get('currentPage') -1 ]);
        this.get('controller.model').set('queryResult', this.get('controller.model').get("jobData")[this.get('controller.model').get('currentPage') -1 ]);
        this.get('controller.model').set('hasNext', true);
        this.get('controller.model').set('hasPrevious', (this.get('controller.model').get('currentPage') > 1) ? true : false );
      }

      let existingWorksheets = this.get('store').peekAll('worksheet');
      let myWs = null;
      if(existingWorksheets.get('length') > 0) {
        myWs = existingWorksheets.filterBy('id', payloadTitle.toLowerCase()).get('firstObject');
      }

      this.transitionTo('queries.query.loading');

      Ember.run.later(() => {
        this.transitionTo('queries.query.results', myWs);
      }, 1 * 1000);
    },
    confirmWorksheetClose(index, id) {
      let existingWorksheets = this.store.peekAll('worksheet');
      let selectedWorksheet = existingWorksheets.filterBy('id', id.toLowerCase()).get('firstObject');
      if(selectedWorksheet.get("isQueryDirty")){
        this.transitionTo('queries.query', id);
      }
      Ember.run.later(() => {
         this.send('closeWorksheet', index, id);
      }, 1 * 100);
    },
    openWorksheetModal(){
      let originalQuery = this.get('controller').get('currentQuery');
      if(Ember.isBlank(originalQuery)) {
        this.get('logger').danger('Query cannot be empty.');
        this.send('resetDefaultWorksheet', this.get('controller.model'));
        return;
      }
      let wf = this.store.peekAll('worksheet').filterBy('id', this.paramsFor('queries.query').worksheetId.toLowerCase()).get('firstObject');
      this.get('controller').set('worksheetTitle', wf.get('title'));
      this.get('controller').set('showWorksheetModal', true);
    },
    closeWorksheet(index, id) {
      let existingWorksheets = this.store.peekAll('worksheet');
      let selectedWorksheet = existingWorksheets.filterBy('id', id.toLowerCase()).get('firstObject');
      this.store.unloadRecord(selectedWorksheet);
      this.controllerFor('queries').set('worksheets', this.store.peekAll('worksheet'));
      let idToTransition = 0;
      if(selectedWorksheet.get('selected')) {
        if(index){
          idToTransition = existingWorksheets.get('content')[parseInt(index)-1].id;
        } else {
          idToTransition = existingWorksheets.get('content')[1].id;
        }
        this.transitionTo('queries.query', idToTransition);
      } else {
        idToTransition = existingWorksheets.get('content')[existingWorksheets.get('length')-1].id;
      }
    },

     closeWorksheetSavePanel(){
      this.get('controller.model').set('worksheetModalSuccess');
      this.get('controller').set('worksheetModalSuccess');
      this.get('controller.model').set('worksheetModalFail');
      this.get('controller').set('worksheetModalFail');
      this.get('controller').set('savedQueryErrorMsg');
      Ember.$("#save-as").dropdown("toggle");
    },

    saveWorksheetModal(){
      this.logGA('SAVEDQUERIES_CREATE');
      console.log('I am in saveWorksheetModal');
      let newTitle = Ember.$('#worksheet-title').val();
      newTitle = Ember.$.trim(newTitle);

      if(this.validateSavedQuery(newTitle)){
        return;
      }

      this.get('controller').set('savedQueryErrorMsg');

      let currentQuery = this.get('controller.model').get('query');
      let selectedDb = this.get('controller.model').get('selectedDb');
      let owner = this.get('controller.model').get('owner');

      let logFile = this.get('controller.model').get('logFile');

      let newSaveQuery = this.get('store').createRecord('saved-query',
        { selectedDatabase:selectedDb,
          title:newTitle,
          owner: owner,
          query: (currentQuery.length > 0) ? currentQuery : ";"
        });


      newSaveQuery.save().then((data) => {
        console.log('saved query saved');

        this.get('controller.model').set('title', newTitle);
        this.get('controller.model').set('isQueryDirty', false);
        this.get('controller').set('worksheetModalSuccess', true);

        Ember.run.later(() => {
          this.get('controller.model').set('worksheetModalSuccess');
          this.get('controller').set('worksheetModalSuccess');
          this.get('controller.model').set('worksheetModalFail');
          this.get('controller').set('worksheetModalFail');
          this.get('controller').set('savedQueryErrorMsg');
          Ember.$("#save-as").dropdown("toggle");
        }, 2 * 1000);
      });
    },

    closeWorksheetModal(){
      this.closeWorksheetAfterSave();
      this.get('controller.model').set('tabDataToClose', null);
  },

    expandQueryEdidorPanel(){
      Ember.$('.query-editor-panel').toggleClass('query-editor-full-width');
      if(!this.get('isQueryEdidorPaneExpanded')) {
        this.set('isQueryEdidorPaneExpanded', true);
      } else {
        this.set('isQueryEdidorPaneExpanded', false);
      }
      Ember.$('.worksheet-container').toggleClass('full-width');
      //Ember.$('.database-panel').toggleClass("hide");
      this.send("queryEditorMaximised");
    },

    expandQueryResultPanel(){
      if(!this.get('controller.model').get('currentJobId')){
       return;
      }
      if(this.get('controller.model').get('queryEditorMaximised')){
        this.get('controller.model').set('queryEditorMaximised', false);
      } else {
        this.get('controller.model').set('queryEditorMaximised', true);
      }
      //Ember.$('.worksheet-container').toggleClass('full-width');
      Ember.$('.editor-min-icon').toggleClass('fa-angle-double-down');
      Ember.$('.multiple-database-search').toggleClass("hide");
    },

    adjustPanelSize(){
      let isFullHeight = (Ember.$(window).height() ===(parseInt(Ember.$('.ember-light-table').css('height'), 10)) ) || false;
      if(!isFullHeight){
        Ember.$('.ember-light-table').css('height', '100vh');
      }else {
        Ember.$('.ember-light-table').css('height', '70vh');
      }
    },

    createQuery(udfName, udfClassname, fileResourceName, fileResourcePath){
      let query = "add jar "+ fileResourcePath + ";\ncreate temporary function " + udfName + " as '"+ udfClassname+ "';";
      this.get('controller').set('currentQuery', query);
      this.get('controller.model').set('query', query );
    },
    openAsWorksheet(savedQuery){
      let hasWorksheetModel = this.modelFor('queries'),
        self = this;
      let worksheetId;

      savedQuery = Ember.Object.create(savedQuery);

      if (Ember.isEmpty(hasWorksheetModel)){
        worksheetId = 1;
      }else {

        let isWorksheetExist = (this.controllerFor('queries').get('worksheets').filterBy('id', savedQuery.get('id')).get('length') > 0);
        if(isWorksheetExist) {
          this.transitionTo('queries.query', savedQuery.get('id'));
          return;
        }

        let worksheets = this.controllerFor('queries').get('worksheets');
        worksheets.forEach((worksheet) => {
          worksheet.set('selected', false);
        });
        worksheetId = `worksheet${worksheets.get('length') + 1}`;
      }
      var isTabExisting = this.store.peekRecord('worksheet', savedQuery.get('id'));
      if(isTabExisting) {
        self.transitionTo('queries.query', isTabExisting.get("id"));
        return;
      }

      let localWs = {
        id: savedQuery.get('id'),
        title: savedQuery.get('title'),
        query: savedQuery.get('query'),
        selectedDb : savedQuery.get('selectedDatabase'),
        owner: savedQuery.get('owner'),
        selected: true
      };
      self.store.createRecord('worksheet', localWs );
      self.controllerFor('queries').set('worksheets', self.store.peekAll('worksheet'));
      self.transitionTo('queries.query', savedQuery.get('id'));
    },
    openDeleteSavedQueryModal(id){
      this.get('controller').set('showDeleteSaveQueryModal', true );
      this.get('controller').set('selectedSavedQueryId', id );
    },
    deleteSavedQueryDeclined(){
      this.get('controller').set('selectedSavedQueryId', null);
      this.get('controller').set('showDeleteSaveQueryModal', false );
    },
    deleteSavedQuery(){
      this.logGA('SAVEDQUERIES_DELETE');

      let self = this;
      let queryId = this.get('controller').get('selectedSavedQueryId');

      console.log('deleteSavedQuery', queryId);
      this.get('store').findRecord('saved-query', queryId, { backgroundReload: false }).then(function(sq) {
        sq.deleteRecord();
        sq.save().then(() => {
          self.send('refreshSavedQueryListAfterDeleteQuery', queryId);
          self.send('closeDeleteSavedQueryModal');
        })
      });
    },
    refreshSavedQueryListAfterDeleteQuery(queryId){
      var updatedList = this.get('controller').get('savedQuerylist').filter(function(item){
        return !(item.get('id').toString() == queryId.toString());
      })
      this.get('controller').set('savedQuerylist', updatedList);
    },
    closeDeleteSavedQueryModal(){
      this.get('controller').set('showDeleteSaveQueryModal', false );
      this.get('controller').set('selectedSavedQueryId', null );
    },
    showSavedquery() {
      this.get('controller').set('isShowSavedquery', true);
      this.get('controller').set('isShowUDF', false);
    },
    showUDF() {
      this.get('controller').set('isShowSavedquery', false);
      this.get('controller').set('isShowUDF', true);
    },
    showRemoveUdfModal(udfId) {
      console.log('udfId', udfId);
      this.get('controller').set('showDeleteUdfModal', true);
      this.get('controller').set('udfId', udfId);
    },
    cancelUdf() {
      this.get('controller').set('showDeleteUdfModal', false);
      this.get('controller').set('udfId', null);
    },
    removeUdf() {
      this.logGA('UDF_DELETE');
      let udfId = this.get('controller').get('udfId');
      let record = this.get('store').peekRecord('udf', udfId);
      if (record) {
        record.destroyRecord().then(() => {
          let udfId = this.get('controller').get('udfId');
          let updatedUdflist = this.get('controller').get('allUDFList').filter((item) => {
            if (!(item.id == udfId)) {
              return item;
            }
          });
          this.get('controller').set('allUDFList', updatedUdflist);
          this.send('cancelUdf');
        })
      }
    }
  }

});
