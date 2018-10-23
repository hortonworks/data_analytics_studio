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

var TABLES = [{
  name: "airports",
  destinationTableName: "airports",
  columns: [{
    name: "iata",
    type: "string",
    sample: "SFO"
  },{
    name: "airport",
    type: "string",
    sample: "San Francisco International Airport"
  },{
    name: "city",
    type: "string",
    sample: "San Francisco"
  },{
    name: "state",
    type: "string",
    sample: "California"
  },{
    name: "country",
    type: "string",
    sample: "U S A"
  },{
    name: "lat",
    type: "double",
    sample: "37.618889"
  },{
    name: "lon",
    type: "double",
    sample: "-122.375"
  }]
}, {
  name: "airlines",
  destinationTableName: "airlines",
  columns: [{
    name: "code",
    type: "string",
    sample: "IAC"
  },{
    name: "description",
    type: "string",
    sample: "Indian Airlines"
  }]
}, {
  name: "planes",
  destinationTableName: "planes",
  columns: [{
    name: "tailnum",
    type: "string",
    sample: "B1235"
  }, {
    name: "owner_type",
    type: "string",
    sample: "owner_type_1"
  }, {
    name: "manufacturer",
    type: "string",
    sample: "Boeing"
  }, {
    name: "issue_date",
    type: "string",
    sample: "03-Feb-2012"
  }, {
    name: "model",
    type: "string",
    sample: "737-900 pax"
  }, {
    name: "status",
    type: "string",
    sample: "status_1"
  }, {
    name: "aircraft_type",
    type: "string",
    sample: "aircraft_type_1"
  }, {
    name: "engine_type",
    type: "string",
    sample: "Jets"
  }, {
    name: "year",
    type: "int",
    sample: "2012"
  }]
}, {
  name: "flights",
  destinationTableName: "flights",
  columns: [{
    name: "DateOfFlight",
    type: "date",
    sample: "28-Dec-2017"
  },{
    name: "DepTime",
    type: "int",
    sample: "1514470989"
  }, {
    name: "CRSDepTime",
    type: "int",
    sample: "1514470989"
  }, {
    name: "ArrTime",
    type: "int",
    sample: "1514470989"
  }, {
    name: "CRSArrTime",
    type: "int",
    sample: "1514470989"
  }, {
    name: "UniqueCarrier",
    type: "string",
    sample: "uc1"
  }, {
    name: "FlightNum",
    type: "int",
    sample: "123"
  }, {
    name: "TailNum",
    type: "string",
    sample: "123"
  }, {
    name: "ActualElapsedTime",
    type: "int",
    sample: "123"
  }, {
    name: "CRSElapsedTime",
    type: "int",
    sample: "123"
  }, {
    name: "AirTime",
    type: "int",
    sample: "123"
  }, {
    name: "AirTime",
    type: "int",
    sample: "123"
  }, {
    name: "ArrDelay",
    type: "int",
    sample: "123"
  }, {
    name: "DepDelay",
    type: "int",
    sample: "123"
  }, {
    name: "Origin",
    type: "string",
    sample: "123"
  }, {
    name: "Dest",
    type: "string",
    sample: "123"
  }, {
    name: "Distance",
    type: "int",
    sample: "123"
  }, {
    name: "TaxiIn",
    type: "int",
    sample: "123"
  }, {
    name: "TaxiOut",
    type: "int",
    sample: "123"
  }, {
    name: "Cancelled",
    type: "int",
    sample: "123"
  }, {
    name: "CancellationCode",
    type: "varchar (1)",
    sample: "123"
  }, {
    name: "Diverted",
    type: "varchar (1)",
    sample: "123"
  }, {
    name: "CarrierDelay",
    type: "int",
    sample: "123"
  }, {
    name: "WeatherDelay",
    type: "int",
    sample: "123"
  }, {
    name: "NASDelay",
    type: "int",
    sample: "123"
  }, {
    name: "SecurityDelay",
    type: "int",
    sample: "123"
  }, {
    name: "LateAircraftDelay",
    type: "int",
    sample: "123"
  }]
}];

export default Ember.Controller.extend({

  data: Ember.Object.create({
    // name: "Def Name",
    // description: "Def Decsription",

    // tableName: "tableName",
    // dataStore: "dataStore",
    // includePath: "includePath",
    // dataStore: "dataStore",

    // dataLake: "dataLake",
    // metaStore: "metaStore",
    //
    // securityPolicy: "securityPolicy",
    // securityTag: "securityTag",
  }),

  init: function () {
    this.resetData();
  },

  resetData: function () {
    this.set("data", Ember.Object.create({
      selectedTables: JSON.parse(JSON.stringify(TABLES)),

      dbCount: 1,
      instanceCount: 0,
      capacity: (Math.random() * 10 ).toFixed(2),
      iamRole: "AWS-S3-Security",

      dataStore: "Amazon S3"
    }));
  },

  dataLakeLocations: ["", "San Francisco", "New York", "London", "Dubai", "Bangalore", "Bejing", "Tokyo"],
  dataStores: [{
    name: "HDFS",
    icon: "/assets/images/storages/hdfs.png"
  }, {
    name: "Amazon S3",
    icon: "/assets/images/storages/s3.png"
  }, {
    name: "Windows Azure Blob Storage",
    icon: "/assets/images/storages/azure.png"
  }, {
    name: "Windows Azure Datalake Store",
    icon: "/assets/images/storages/azure.png"
  },{
    name: "Google Cloud Storage",
    icon: "/assets/images/storages/gcloud.png"
  },{
    name: "Cloud Files",
    icon: "/assets/images/storages/rackspace.png"
  },{
    name: "Connectria Cloud Storage",
    icon: "/assets/images/storages/connectria.png"
  }],
  iamRoles: {
    "HDFS": ["HDFS-Role1", "HDFS-Role2", "HDFS-Role3"],
    "Amazon S3": ["AWS-S3-Security", "AWS-S3-Role1", "AWS-S3-Role2", "AWS-S3-Role3"],
    "Windows Azure Blob Storage": ["WAS-Role1", "WAS-Role2", "WAS-Role3"],
    "Windows Azure Datalake Store": ["WAS-Role1", "WAS-Role2", "WAS-Role3"],
    "Google Cloud Storage": ["GCS-Role1", "GCS-Role2", "GCS-Role3"],
    "Cloud Files": ["CF-Role1", "CF-Role2", "CF-Role3"],
    "Connectria Cloud Storage": ["CCS-Role1", "CCS-Role2", "CCS-Role3"]
  },

  dbNames: ["", "flight-db"],
  storageLocations: ["", "sf-store", "ny-store", "london-store", "bangalore-store", "bejing-store", "tokyo-store"],
  stagingLocations: ["Local.storage", "XXX.storage"],
  externalLocations: ["external.location", "XXX.location"],

  datatypes1: ["Abc", "Def"],

    datatypes: [{
    id: 1,
    label: "Abc"
  }, {
    id: 2,
    label: "Def"
  }, {
    id: 3,
    label: "Ghi"
  }, {
    id: 4,
    label: "Jkl"
  }],
  selectedDatatypes: Ember.A(),

  securityPolicies: ["", "Security Only", "Compliance Team", "Policy-1", "Policy-2", "Policy-3"],
  securityTags: ["", "Confidential-Class-Top", "Tag-1", "Tag-2", "Tag-3"],

  iamRolesForCurrentDataStore: Ember.computed("iamRoles", "data.dataStore", function () {
    return this.get("iamRoles")[this.get("data.dataStore")];
  }),

  setCompleted: function () {
    var elements = $('.nav-pills.main > li');
    var add = true;
    for(var i=0; i<elements.length; i++) {
      var element = $(elements[i]);

      if(element.hasClass("active")) {
        add = false;

        if(element.find("a").attr("href") == "#schema") {
          Ember.run.later(this, "alignSchema", 100);
        }
      }

      if(add) {
        element.addClass("completed");
      }
      else {
        element.removeClass("completed");
      }
    }
  },

  alignSchema: function () {
    Ember.$(".schema-table input").each(function (index, element) {
      var input = Ember.$(element);
      input.width(input.siblings(".dummy").outerWidth() + 15);
    });
    Ember.$(".schema-tr select").each(function (index, element) {
      var input = Ember.$(element);
      input.width(input.siblings(".dummy").outerWidth() + 15);
    });

    Ember.$(".schema-tr input").on("keyup", function (event) {
      var input = Ember.$(event.target);
      input.width(input.siblings(".dummy").width() + 15);
    });
    Ember.$(".schema-tr select").on("change", function (event) {
      var input = Ember.$(event.target);
      input.width(input.siblings(".dummy").width() + 15);
    });
  },

  actions: {
    setCompleted: function () {
      Ember.run.later(this, "setCompleted", 100);
    },
    previous: function () {
      $('.nav-pills.main > .active').prev('li').find('a').trigger('click');
      this.setCompleted();
    },
    next: function () {
      $('.nav-pills.main > .active').next('li').find('a').trigger('click');
      this.setCompleted();
    },
    updateTables: function () {
      this.get("data.selectedTables").pushObject(Ember.Object.create({
        name: "airports"
      }));
    },
    typeSelectionMade: function () {}
  }

});
