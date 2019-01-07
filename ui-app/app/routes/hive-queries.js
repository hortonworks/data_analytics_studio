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
import NameMixin from '../mixins/name';
import commons from '../mixins/commons';

import { BASE } from '../components/range-panel';

const REFRESH = {refreshModel: true};

export default Ember.Route.extend(commons, {
  title: "Hive Queries",
  breadCrumb: {
    title: 'Queries'
  },
  propertyMap: Ember.computed(function () {
    return this.get("store").serializerFor("indexed-query").get("maps");
  }),

  getRangeTime: function (range) {
    var from = moment(),
      to = moment();

    if(range.from !== -1) {
      from.startOf(range.base);
      from.subtract(range.from, range.base)
    }

    if(range.to !== -1) {
      to.endOf(range.base);
      to.subtract(range.to, range.base)
    }

    return {
      fromTime: from.utc().valueOf(),
      toTime: to.utc().valueOf()
    };
  },

  createQuery() {
    var tableDefinition = this.get("controller.definition"),
        propertyMap = this.get("propertyMap"),
        rangeData = tableDefinition.get("rangeData");

    var query = {
      type: "BASIC",

      offset: (tableDefinition.get("pageNum") - 1) * tableDefinition.get("rowCount"),
      limit: tableDefinition.get("rowCount"),
    };

    if(tableDefinition.get("searchText")) {
      query.text = tableDefinition.get("searchText");
    }

    if(tableDefinition.get("facetConditions")) {
      query.facets = this.normalizeFacetRequest(tableDefinition.get("facetConditions"));
    }

    if(tableDefinition.get("sortColumnId")) {
      query.sortText = propertyMap[tableDefinition.get("sortColumnId")] + ":"+ (tableDefinition.get("sortOrder").toUpperCase() || "ASC");
    }

    if(rangeData) {
      if(rangeData.base !== BASE.CUSTOM) {
        rangeData = this.getRangeTime(rangeData);
      }

      query.startTime = rangeData.fromTime;
      query.endTime = rangeData.toTime;
    }

    return query;
  },

  normalizeFacetRequest: function (facetConditions) {
    var facets = [];

    for(var facetName in facetConditions) {
      facets.push({
        field: facetName,
        values: facetConditions[facetName].in
      });
    }

    return facets;
  },

  fieldsInformation: [],

  model: function(params/*, transition*/) {
    this.closeAutocompleteSuggestion();
    this.setActiveTab('hive-queries');
    Ember.run.debounce(this, "loadRecords", 10);
    Ember.run.debounce(this, "loadFacets", 10);
  },

  loadRecords: function () {
    var that = this;
    this.set("controller.definition.isLoading", true);
    this.get("loader").query('indexed-query', this.createQuery(), {reload: true}).then(function (data) {
      that.set("controller.definition.isLoading", false);
      that.set("controller.model", data);
    });
  },

  normaliseFacetValues: function (facet) {
    let values = facet.get("values");

    if(values) {
      values = values.filter(function (value) {
        return value.key;
      });

      let fieldName = facet.get("data.fieldName");
      if(fieldName == "tablesRead" || fieldName == "tablesWritten") {
        values.forEach(function (value) {
          let [dbName, tableName] = value.key.split(".");
          value.displayText = `${tableName} (${dbName})`;
        });
      }

    }

    return values;
  },

  killQuery: function(record){
    this.store.adapterFor('indexed-query').killQuery(record.get('queryID'))
    .then((data)=>{
      console.log('Query kill is successful.');
      record.set('status', 'KILLED');
    }).catch((error) =>{
      console.log('Query kill fails.');
      /* TODOs:: Handle this with flash messeges. */
    })
  },

  loadFacetBatch: function (query, facetFields, facetData) {
    var facetQuery = {
      text: query.text,
      facetFields: facetFields.join(","),
      startTime: query.startTime,
      endTime: query.endTime
    },
    that = this;

    return this.get("loader").query('facet', facetQuery, {reload: true}, {
      "entity_type": "query"
    }).then(function (data) {
      data.forEach(function (facet) {
        facetData.set(facet.get("fieldName"), that.normaliseFacetValues(facet));
        facetData.incrementProperty("fieldCount");
      });
    }, function(error){
        that.set("controller.apiAcessError", true);
        that.set("controller.error", error.errors[0].detail);
    });
  },

  loadFacets: function () {
    // Facetable fields status, queueName, userId, requestUser, executionMode, dagName, applicationId, callerId
    var that = this,
        query = this.createQuery(),
        facetData = Ember.Object.create({
          fieldCount: 0
        });

    this.set("controller.dataLoader.facets", facetData);

    this.store.findAll('fields-information')
    .then(data => {
      var fieldsInfoArr = [];
      Ember.A(data.toArray()).forEach(data => {
        let localFieldsInfo = {
          id: data.get('id'),
          fieldName: data.get('fieldName'),
          facetable: data.get('facetable'),
          displayName: data.get('displayName'),
          searchable: data.get('searchable'),
          sortable: data.get('sortable'),
          rangeFacetable: data.get('rangeFacetable')
        };
        fieldsInfoArr.push(localFieldsInfo);
      });
      this.set('fieldsInformation', fieldsInfoArr);

      var facetableFieldsInfo = this.get('fieldsInformation').filterBy('facetable', true);

      while(facetableFieldsInfo.length) {
        var localTriplet = facetableFieldsInfo.splice(0,3).map((data)=>{
          return data.fieldName;
        });
        this.loadFacetBatch(query, localTriplet, facetData);
      }

      //this.loadFacetBatch(query, ["status", "queueName", "requestUser"], facetData);
      //this.loadFacetBatch(query, ["executionMode"], facetData);

    }, function(reason) {
         that.set("controller.apiAcessError", true);
         try {
            that.set("controller.error", reason.errors[0].detail);
         } catch(e) {}
    });
  },

  loadSuggestedSearches: function () {
    var that = this, self = this;
    this.get("loader").query('suggested-search', {entityType: 'query'}, {reload: true}).then(function (data) {
      that.set("controller.searches", data);
    }, function(error){
      self.set("controller.apiAcessError", true);
      self.set("controller.error", self.extractError(error.errors[0].detail));
    });
  },

  setupController: function (controller, model) {
    this.logGA('QUERIES');
    controller.set("isLoading", false);
    controller.set("query1", {});
    controller.set("query2", {});
    this.loadSuggestedSearches();
  },

  getDominantDatabase(databasesUsed){
    let databasesUsedFO = databasesUsed.get('firstObject');
    let databasesSorted = Object.keys(databasesUsedFO).map( obj =>{
      return {db: obj, occurences: databasesUsedFO[obj] };
    })
    .sort((a, b)=>{
      return b["occurences"] - a["occurences"];
    });

    if(databasesSorted.length > 1 ){
      if(databasesSorted[0].db == "default" && databasesSorted[0].occurences == databasesSorted[1].occurences){
        return databasesSorted[1].db; 
      } else{
        return databasesSorted[0].db;
      }
    } else{
      return databasesSorted[0].db;
    }
  },

  actions: {
    tableDefinitionUpdated: function () {
      Ember.run.debounce(this, "loadRecords", 10);
    },
    searchTextUpdated: function () {
      Ember.run.debounce(this, "loadFacets", 10);
    },
    setRange: function (range) {
      this.set("controller.definition.rangeData", range);
      Ember.run.debounce(this, "loadRecords", 10);
      Ember.run.debounce(this, "loadFacets", 10);
    },

    saveSearch: function (search) {
      var that = this;
      var searchRecord = this.get("store").createRecord('suggested-search', search);
      searchRecord.save().then(function () {
        that.loadSuggestedSearches();
      });
    },
    deleteSearch: function (search) {
      search.destroyRecord();
    },

    setBreadcrumbs: function () {
    },
    compareQuery: function (record) {
      if(!this.get('controller').get('query1.queryText')) {
        this.get('controller').set('query1', record);
      } else if(!this.get('controller').get('query2.queryText')) {
        this.get('controller').set('query2', record);
      }
    },
    clearQuery(id) {
      this.get('controller').set(id, {});
    },
    swapQuery() {
      let query1 = this.get('controller').get('query1'), query2 = this.get('controller').get('query2');
      this.get('controller').set('query1', query2);
      this.get('controller').set('query2', query1);
    },
    showComparision() {
      let query_1 = this.get('controller').get('query1.queryID'), query_2 = this.get('controller').get('query2.queryID');
      this.transitionTo('query-diff', {
        queryParams: {
          query_1,
          query_2
        }
      });
    },
    killQuery: function(record){
      //console.log('queryId hive-queries route: ', queryId);
      this.killQuery(record);
    },
    editQuery: function(query) {
        let existingWorksheets = this.get('store').peekAll('worksheet');
        let newWorksheetName = 'worksheet', id;
        if(!this.controllerFor("queries").worksheetCount && !existingWorksheets.get("length")) {
          newWorksheetName = newWorksheetName + 1;
        } else {
          id = parseInt(this.controllerFor("queries").worksheetCount)+1;
          if(!id){
            id = existingWorksheets.get("length")+1;
          }
          newWorksheetName = newWorksheetName + id;
        }
        if(!existingWorksheets.filterBy('id', "saved").get('firstObject')) {
          this.store.createRecord('worksheet', {
            id: "saved",
            title: "Saved".capitalize(),
            isQueryDirty: false,
            selected: false
          });
        }
        let newWorksheetTitle = newWorksheetName.capitalize();

        let dominantDb = this.getDominantDatabase(query.get('databasesUsed'));

        this.get('store').createRecord('worksheet', {
          id: newWorksheetName,
          title: newWorksheetTitle,
          isQueryDirty: false,
          query: query.get('queryText'),
          selected: true,
          selectedDb : dominantDb
        });
        existingWorksheets.setEach('selected', false);
        this.controllerFor('queries').set('worksheets', this.get('store').peekAll('worksheet'));
        this.controllerFor('queries').set('worksheetCount',parseInt(id));
        this.transitionTo('queries.query', newWorksheetTitle);
    },

    refreshCurrentRoute: function() {
      var tableDefinition = this.get("controller.definition")
      if(tableDefinition.get("searchText")) {
        tableDefinition.set("searchText", '');
      }

      if(tableDefinition.get("facetConditions")) {
        tableDefinition.set("facetConditions", '');
      }
      this.set("controller.apiAcessError", false);
      this.set("controller.error", {});
      this.refresh();
    }

  }

});
