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
import config from '../config/environment';

let leftMenuTabs = [
  Ember.Object.create({
    'name':'Queries',
    'fa-icon':'search',
    'link': 'hive-queries',
    'header': 'Search'
  }),
  Ember.Object.create({
    'name':'Compose',
    'fa-icon':'edit',
    'link': 'queries',
    'header': 'Worksheets'
  }),
  Ember.Object.create({
    'name':'Database',
    'fa-icon':'database',
    'link': 'databases',
    'header': 'Databases'
  })
];

if(config.APP.DASLITE == false) {
  leftMenuTabs.push(Ember.Object.create({
    'name':'Reports',
    'fa-icon':'area-chart',
    'link': 'reports',
    'header': 'Reports'
  }));
}

if(config.APP.CLOUD == true) {
  leftMenuTabs.unshift(Ember.Object.create({
    'name':'Warehouses',
    'fa-icon':'cubes',
    'link': 'warehouses',
    'header': 'Warehouses'
  }));
}
if(!config.APP.DISABLE_UDF_SETTINGS) {
  leftMenuTabs.push(Ember.Object.create({
    'name':'Manage',
    'fa-icon':'wrench',
    'link': 'settings',
    'header': 'Manage'
  }));
}

let homeIcon =  Ember.Object.create({
    'name':'Queries',
    'fa-icon':'search',
    'link': 'hive-queries',
    'header': 'Queries',
    'projectname': '<b class="project-header-block">DATA ANALYTICS</b> <span class="project-header-small">STUDIO</span>',
    'icon': 'hive-studio.png'
});

export default {leftMenuTabs:leftMenuTabs, homeIcon:homeIcon};
