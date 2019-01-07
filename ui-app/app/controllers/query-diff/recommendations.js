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
   query: Ember.inject.service(),
   recommendationList1 : Ember.computed('model' , function () {
     this.set('loadingRecommendations1', true);
     var self = this;
     return this.get('query').getRecommendations(this.get('model')[0]).then((data) => {
        this.set('loadingRecommendations1', false);
        self.set('isErrorWhileFetching1', false);
        self.set("recommendations1", data.recommendations);
        return data.recommendations;
     }, function(data){
        self.set("recommendations1", []);
        self.set('isErrorWhileFetching1', true);
        self.set('loadingRecommendations1', false);
     });
   }),
   recommendationList2 : Ember.computed('model' , function () {
     this.set('loadingRecommendations2', true);
     var self = this;
     return this.get('query').getRecommendations(this.get('model')[1]).then((data) => {
        this.set('loadingRecommendations2', false);
        self.set('isErrorWhileFetching2', false);
        self.set("recommendations2", data.recommendations);
        return data.recommendations;
     }, function(data){
        self.set("recommendations2", []);
        self.set('isErrorWhileFetching2', true);
        self.set('loadingRecommendations2', false);
     });
   }),
   filteredRecommendations: Ember.computed('recommendations1', 'recommendations2', 'showDifference', function() {
    let recommendations1 = this.get('recommendations1'), recommendations2 = this.get('recommendations2');
    if(recommendations1.length && recommendations2.length) {
        if(this.get('showDifference')) {
            if(recommendations1 && recommendations2 && recommendations1.length >= recommendations2.length) {
                return this.filterRecommendations(recommendations1, recommendations2, true);
            } else {
                return this.filterRecommendations(recommendations2, recommendations1, false);
            }
        } else {
            this.filterRecommendations(recommendations1, recommendations2);
        }
    }
    return {recommendations1: this.get('recommendations1'), recommendations2: this.get('recommendations2')};
   }),
   showDifference: false,
   filterRecommendations(arr1, arr2, isOrderReversed) {
    let a = JSON.parse(JSON.stringify(arr1)), b = JSON.parse(JSON.stringify(arr2)), c = [];
     for (var i = 0, len = a.length; i < len; i++) {
        for (var j = 0, len2 = b.length; j < len2; j++) {
            if (a[i].message.indexOf(b[j].message) === 0) {
                b.splice(j, 1);
                len2=b.length;
                a[i] = null;
                break;
            }
        }
        if(a[i]) {
            c.push(a[i]);
        }
    }
    if(isOrderReversed) {
        return {recommendations1: c, recommendations2: b}
    } else {
        return {recommendations1: b, recommendations2: c}
    }
   }
});