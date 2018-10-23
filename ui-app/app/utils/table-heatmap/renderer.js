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

export default function doRender(data, selector, draggable) {

const width = '100%', height = '1000';
var zoomInit = null;
d3.select(selector).select('*').remove();

    const breakIndex= parseInt (($(window).width() - $('.left-pane').width() )/200);

    const svg = d3.select(selector)
        .append('svg')
        .attr('width', width)
        .attr('height', height);

    const g = svg.selectAll('.someClass')
              .data(data)
              .enter()
              .append("foreignObject")
              .attr("id", function(d) {
                  return d.tableName;
              })
              .attr("class", "table-node")
              .attr("X", function(d, index) {
                  return 200*(index%breakIndex);
              })
              .attr("Y", function(d, index) {
                  return parseInt(index/breakIndex)*300;
              })
              .attr("transform", function(d, index) {
                  return "translate(" + 200*(index%breakIndex) + "," + parseInt(index/breakIndex)*300  + ")";
              })
            .append("xhtml:table")
             .attr("width", function(d) { return 150;})
             .attr("class", "table-db")
              .selectAll('tr')
              .data(function(row, i) { return row.columns;})
             .enter()
              .append('tr')
             .attr("class", "table-row");

             g.append("td")
                .data(function(row, i) {
                return row.columns;
                })
              .text(function(d) {
                return Object.keys(d)[0];
              })
              .attr('class', 'columnValue')
              .enter();

var links = [
   {
     source: "Supplier",
     target: "SuppPersDtls1"
   },
   {
     source: "Product",
     target: "Product1"
   }, {
     source: "SuppPers",
     target: "SuppPersDtls"
   },
   {
     source: "Order1",
     target: "SuppPers2"
   }];


var allNodes = $('.table-node');

var yOffSet = 20;
var xOffSet = 75;

var links = svg.selectAll("link")
   .data(links)
   .enter()
   .append("line")
   .attr("class", "link")
   .attr("x1", function(l) {
     var sourceNode = allNodes.filter(function(d, i) {
       return i.id == l.source
     })[0];

     d3.select(this).attr("y1", parseInt($(sourceNode)[0].attributes[3].value) + yOffSet );
     return parseInt($(sourceNode)[0].attributes[2].value) + xOffSet ;
   })
   .attr("x2", function(l) {
     var targetNode = allNodes.filter(function(d, i) {
       return i.id == l.target
     })[0];

     d3.select(this).attr("y2", parseInt($(targetNode)[0].attributes[3].value) + yOffSet);
     return parseInt($(targetNode)[0].attributes[2].value) + xOffSet;
   })
   .attr("fill", "none");



}

