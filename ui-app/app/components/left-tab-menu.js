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
export default Ember.Component.extend({
  index : 2,
  showMenuLabel: true,
  togglerData : [Ember.Object.create({
    'fa-icon':'fa-angle-double-left',
  }), Ember.Object.create({
    'fa-icon':'fa-angle-double-right',
    'toggler-class': 'menu-toggler-min col-sm-1'
  })],
  toggler : Ember.Object.create({
    'fa-icon':'fa-angle-double-left',
    'toggler-class': 'col-sm-2'
  }),

  didRender() {
    this._super(...arguments);
    //this.highlightTab(this.get('tabs.leftMenuTabs')[0]);
  },
  highlightTab(tab){
      this.$(".menu").removeClass("active");
      this.$(".sub-menu").removeClass("active");
      this.$(".sub-menu .list-group-item").removeClass("active");
      this.$(".left-green-arrow").addClass("hide");
      this.$(".menu-"+tab.link).find(".left-green-arrow").first().removeClass("hide");
      this.$(".menu-"+tab.link).addClass("active");
      this.sendAction("setHeaderTitles", tab);
  },
  actions : {
    toggleSubMenu(tab) {
      Ember.$(".sub-menu ul:not(."+tab.link+")").addClass("hide")
      Ember.$("."+tab.link).toggleClass("hide");

      if(Ember.$(".menu-"+tab.link).find(".fa-angle-up.hide").length) {
       Ember.$(".menu-"+tab.link).find(".fa-angle-up").removeClass("hide");
       Ember.$(".menu-"+tab.link).find(".fa-angle-down").addClass("hide");
      } else {
       Ember.$(".menu-"+tab.link).find(".fa-angle-down").removeClass("hide");
       Ember.$(".menu-"+tab.link).find(".fa-angle-up").addClass("hide");
      }
    },
    setRouteParam(tab) {
      this.highlightTab(tab);
    },
    setHeaderTitles(tab) {
      this.sendAction("setHeaderTitles", tab);
    },
    toggleSlider() {
      this.incrementProperty('index');
      let togglerData = this.get('togglerData'), className;
      if(this.get('index')%2 !== 0){
        this.set('toggler', togglerData[1]);
        this.set('showMenuLabel', false);
        this.sendAction('setSliderWidth', 'col-sm-1 left-menu-icons', 'col-sm-11 right-menu-expanded');
      } else {
        this.set('toggler', togglerData[0]);
        this.set('showMenuLabel', true);
        this.sendAction('setSliderWidth', 'col-sm-2', 'col-sm-10');
      }
      console.log(this.get('toggler'));
    }
  }
});
