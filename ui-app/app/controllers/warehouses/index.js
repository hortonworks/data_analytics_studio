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

export default Ember.Controller.extend({

  warehouses: Ember.A([Ember.Object.create({
    name: "Customers",
    location: "San Francisco",
    description: "Customer information data stores.",
    dbCount: 4,
    instanceCount: 1,
    capacity: 23
  }), Ember.Object.create({
    name: "Inventory",
    location: "New York",
    description: "Inventory tracking and historical records.",
    dbCount: 23,
    instanceCount: 3,
    capacity: 14
  }), Ember.Object.create({
    name: "Finance",
    location: "London",
    description: "Financial information for org.",
    dbCount: 17,
    instanceCount: 4,
    capacity: 23
  })]),

  instances: Ember.A([{
    size: "SMALL",
    upTime: 22,
    utilization: 81,
    currentUserCount: 3,
    bgClass: "color1"
  }, {
    size: "LARGE",
    upTime: 100,
    utilization: 20,
    currentUserCount: 10,
    bgClass: "color2"
  }, {
    size: "SMALL",
    upTime: 30,
    utilization: 75,
    currentUserCount: 2,
    bgClass: "color3"
  }]),

  instanceTemplates: Ember.A([{
    size: "Small",
    color: "color1",
    capacity: 1,
    concurrentUserCount: 5
  }, {
    size: "Medium",
    color: "color2",
    capacity: 10,
    concurrentUserCount: 25
  }, {
    size: "Large",
    color: "color3",
    capacity: 30,
    concurrentUserCount: 100
  }, {
    size: "X Large",
    color: "color4",
    capacity: 100,
    concurrentUserCount: 500
  }]),

  actions: {
    setProperties: function(){
      for(let i = 0; i< arguments.length; i += 2) {
        this.set(arguments[i], arguments[i + 1]);
      }
    },
    deleteWarehouse: function (warehouse) {
      this.get("warehouses").removeObject(warehouse);
    },

    showAttachInstance: function (warehouse) {
      this.set("currentSelectedWarehouse", warehouse);
      this.set("showAttachInstance", true);
    },
    connectInstance: function (instance) {
      this.get("currentSelectedWarehouse").incrementProperty("instanceCount");
      this.set("showAttachInstance", false);
      return true;
    },
    addInstance: function (instanceTemplate) {
      this.setProperties({
        showCreateInstance: false,
        showAttachInstance: true
      });
      console.log(instanceTemplate);
      this.get("instances").pushObject({
        size: instanceTemplate.size,
        upTime: 0,
        utilization: 0,
        currentUserCount: 0,
        bgClass: "color4"
      });
    }
  }

});
