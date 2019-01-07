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
  classNames: ['list-filter'],
  header: '',
  subHeader: '',
  caseInsensitive: true,

  currentPage: 1,
  totalPages: 1,
  prevFilterText: '', 
  defaultFacetsCount: 100,

  paginatedFacets: null,
  visibleFacetsChunk: null,

  arrayChunks: (array, chunk_size) => {
   if(!array) return Ember.A();   
   return Array(Math.ceil(array.length / chunk_size)).fill().map((_, index) => index * chunk_size).map(begin => array.slice(begin, begin + chunk_size))
  },

  isNextPage: Ember.computed("currentPage", "totalPages", function(){
    return !(this.get("totalPages") == this.get("currentPage"));
  }), 

  isPrevPage: Ember.computed("currentPage", "totalPages", function(){
    return (this.get("currentPage") != 1);
  }),

  items: [],
  filterText: '',
  emptyFilterText: Ember.computed('filterText', function() {
    return this.get('filterText').length === 0;
  }),
  
  filteredItems: Ember.computed('filterText', 'items.[]', 'currentPage', function() {
    let filteredItems = this.get('items').filter((item) => {
      let filterText = this.get('caseInsensitive') ? this.get('filterText').toLowerCase() : this.get('filterText');
      let itemName = this.get('caseInsensitive') ? item.get('name').toLowerCase() : item.get('name');
      return itemName.indexOf(filterText) !== -1;
    });


    if(!(this.get("filterText") == this.get("prevFilterText"))){
      this.set("currentPage", 1);
      this.set("prevFilterText", this.get("filterText"));
    }

    let paginatedFacets = this.arrayChunks(filteredItems, this.get("defaultFacetsCount"));
    this.set("paginatedFacets", paginatedFacets);
    this.set("totalPages", paginatedFacets.length);

    /* 
    let currentPage = this.get("currentPage");
    paginatedFacets.forEach((facet, index) => {
      facet.forEach( item => {
        if(item.get('selected') == true){
          currentPage = index + 1;
        }
      })
    });
    this.set("currentPage", currentPage);
    */

    let visibleFacetsChunk = this.get("paginatedFacets")[this.get("currentPage") - 1];
    this.set("visibleFacetsChunk", visibleFacetsChunk);

    return visibleFacetsChunk;


  }),

  actions: {
    enableFilter() {
      this.$('input').focus();
    },

    disableFilter() {
      this.set('filterText', '');
    },

    showNext: function () {
      let currentPage = this.get("currentPage");
      this.set("currentPage", currentPage + 1);
    }, 
    showPrev: function () {
      let currentPage = this.get("currentPage");
      this.set("currentPage", currentPage - 1);
    }
    
  }
});
