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
import ColumnDefinition from 'em-table/utils/column-definition';
import TableDefinition from 'em-table/utils/table-definition';

export default Ember.Component.extend({
    store: Ember.inject.service(),
    query: Ember.inject.service(),
    isAutoRefresh: true,
    autoRefreshInterval: 100000,
    didUpdateAttrs() {
      this._super(...arguments);
      if(this.get('selectedDb').length){
        this.set('tableNames', []);
        this.set('selectedDbObj', Ember.Object.create({"name" : this.get('selectedDb'), id : this.get('selectedDb')}));
      }
    },
    didReceiveAttrs() {
      this._super(...arguments);
      let dbDetails = this.get("selectedTablesModels")[0];
      this.refreshDatabasePanel([{dbname:dbDetails.dbName || dbDetails.dbname, id:dbDetails.id}]);
    },
    refreshDatabasePanel(filteredItems) {
      this.get("query").startRefresh();
      this.set('loading', true);
      if(this.get("isAutoRefresh")) {
        this.get("query").refreshTableData(this.synchDatabaseData.bind(this), this.synchTableData.bind(this), this.get("autoRefreshInterval"), filteredItems, ColumnDefinition, this);
      } else {
        this.set("tableNames", []);
      }
    },
    "sortProp": ['id'],
    sortedDatabases : Ember.computed.sort("alldatabases", "sortProp"),

    selectedDbObj: Ember.computed('selectedDb', 'filteredItems', function() {
      return {"name" : this.get('selectedDb'), id : this.get('selectedDb') };
    }),
    tableDefinition: TableDefinition.create({
      minValuesToDisplay: 0,
      enableFaceting: true
    }),
    filteredFacets: Ember.computed("selectedDbObj", "tableNames", "filterText", function () {
      var allFacets = this.get("tableNames.facetedFields"),
          filterText = this.get("filterText"),
          filteredFacets;
      if (filterText) {
        filteredFacets = allFacets.filter(function (facet) {
          return facet.name.match(filterText);
        });
        return {dbname:this.get("tableNames").dbname, facetedFields:filteredFacets};
      } else {
        return this.get("tableNames");
      }
    }),

    synchTableData:function (facetsFields) {
      if(facetsFields.dbname === this.get('selectedDbObj.name')){
        this.set('loading', false);
        if(facetsFields) {
          facetsFields.facetedFields.sort(function(a, b){
            var nameA=a.name.toLowerCase(), nameB=b.name.toLowerCase()
            if (nameA < nameB){ //sort string ascending
              return -1
            }
            if (nameA > nameB){
              return 1
            }
            return 0 //default return value (no sorting)
          });
        }
        this.set("tableNames", facetsFields);
      }
    },
    synchDatabaseData:function (data) {
      try {
        this.set("alldatabases", data);
      } catch(e) {}
    },
    actions : {
      updateTables(option) {
        this.set('tableNames', []);
        this.set('selectedDb', []);
        this.get("query").stopRefresh();
        this.refreshDatabasePanel([{dbname:this.get('selectedDbObj').get("name"), id:this.get('selectedDbObj').get("id")}]);
        this.sendAction('changeDbHandler', [{dbname:this.get('selectedDbObj').get("name"), id:this.get('selectedDbObj').get("id")}]);
      },
      toggleDbPanel() {
        this.$("#db_accordion_expand").toggleClass('hide');
        this.sendAction('expandQueryPanel');
        this.$('.database-panel').toggleClass('hide');
      }
    }
});
