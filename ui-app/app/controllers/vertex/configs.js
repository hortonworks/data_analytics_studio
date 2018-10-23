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

import TableController from '../table';
import ColumnDefinition from 'em-table/utils/column-definition';

var MoreObject = more.Object;

// Better fits in more-js
function arrayfy(object) {
  var array = [];
  MoreObject.forEach(object, function (key, value) {
    array.push({
      key: key,
      value: value
    });
  });
  return array;
}

export default TableController.extend({
  searchText: "tez",

  queryParams: ["configType", "configID"],
  configType: null,
  configID: null,

  breadcrumbs: Ember.computed("configDetails", "configID", "configType", function () {
    var crumbs = [{
      text: "Configurations",
      routeName: "vertex.configs",
      queryParams: {
        configType: null,
        configID: null,
      }
    }],
    type = this.get("configType"),
    name;

    if(this.get("configType")) {
      name = this.get("configDetails.name") || this.get("configDetails.desc");
    }

    if(type && name) {
      type = type.capitalize();
      crumbs.push({
        text: `${type} [ ${name} ]`,
        routeName: "vertex.configs",
      });
    }

    return crumbs;
  }),

  setBreadcrumbs: function() {
    this._super();
    Ember.run.later(this, "send", "bubbleBreadcrumbs", []);
  },

  columns: ColumnDefinition.make([{
    id: 'configName',
    headerTitle: 'Configuration Name',
    contentPath: 'configName',
  }, {
    id: 'configValue',
    headerTitle: 'Configuration Value',
    contentPath: 'configValue',
  }]),

  normalizeConfig: function (config) {
    var userPayload = config.userPayloadAsText ? JSON.parse(config.userPayloadAsText) : {};
    return {
      id: config.name || null,
      name: config.name,
      desc: userPayload.desc,
      class: config.class || config.processorClass,
      initializer: config.initializer,
      configs: arrayfy(userPayload.config || {})
    };
  },

  configsHash: Ember.computed("model.name", "model.dag.vertices", function () {
    var vertexName = this.get("model.name"),

        inputConfigs = [],
        outputConfigs = [],
        vertexDetails;

    if(!this.get("model")) {
      return {};
    }

    vertexDetails = this.get("model.dag.vertices").findBy("vertexName", vertexName);

    (this.get("model.dag.edges") || []).forEach(function (edge) {
      if(edge.outputVertexName === vertexName) {
        let payload = edge.outputUserPayloadAsText;
        inputConfigs.push({
          id: edge.edgeId,
          desc: `From ${edge.inputVertexName}`,
          class: edge.edgeDestinationClass,
          configs: arrayfy(payload ? Ember.get(JSON.parse(payload), "config") : {})
        });
      }
      else if(edge.inputVertexName === vertexName) {
        let payload = edge.inputUserPayloadAsText;
        outputConfigs.push({
          id: edge.edgeId,
          desc: `To ${edge.outputVertexName}`,
          class: edge.edgeSourceClass,
          configs: arrayfy(payload ? Ember.get(JSON.parse(payload), "config") : {})
        });
      }
    });

    return {
      processor: this.normalizeConfig(vertexDetails),

      sources: (vertexDetails.additionalInputs || []).map(this.normalizeConfig),
      sinks: (vertexDetails.additionalOutputs || []).map(this.normalizeConfig),

      inputs: inputConfigs,
      outputs: outputConfigs
    };
  }),

  configDetails: Ember.computed("configsHash", "configType", "configID", function () {
    var configType = this.get("configType"),
        details;

    if(configType) {
      details = Ember.get(this.get("configsHash"), configType);
    }

    if(Array.isArray(details)) {
      details = details.findBy("id", this.get("configID"));
    }

    return details;
  }),

  configs: Ember.computed("configDetails", function () {
    var configs = this.get("configDetails.configs");

    if(Array.isArray(configs)) {
      return Ember.A(configs.map(function (config) {
        return Ember.Object.create({
          configName: config.key,
          configValue: config.value
        });
      }));
    }
  }),

  actions: {
    showConf: function (type, details) {
      this.setProperties({
        configType: type,
        configID: details.id
      });
      Ember.run.later(this, "send", "bubbleBreadcrumbs", []);
    }
  }

});
