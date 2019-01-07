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
import ApplicationAdapter from './application';
import FileUploader from './file-uploader';

export default ApplicationAdapter.extend({
  tableOperations: Ember.inject.service(),

  buildURL: function(){
    return this._super(...arguments);
  },

  buildUploadURL: function (path) {
    return  this.buildURL() + "/upload/" + path;
  },

  uploadFiles: function (path, files, extras) {
    var uploadUrl = this.buildUploadURL(path);

    console.log("uplaoder : uploadURL : ", uploadUrl, " extras : ", extras , "files : ", files);

    var hdrs = Ember.$.extend(true, {},this.get('headers'));
    delete hdrs['Content-Type'];
    var uploader = FileUploader.create({
      headers: hdrs,
      url: uploadUrl
    });

    if (!Ember.isEmpty(files)) {
      var promise = uploader.upload(files[0], extras);
      return promise;
    }
  },

  createTable: function (tableData) {
    console.log("creating table with data :", tableData);
    return this.doPost("createTable",tableData);
  },

  insertIntoTable: function(insertData){
    console.log("inserting into table with data : ", insertData);
    return this.doPost("insertIntoTable",insertData);
  },

  deleteTable: function(deleteData){
    console.log("delete table with info : ", deleteData);
    return this.get('tableOperations').deleteTable(deleteData.database, deleteData.table);
  },

  doPost : function(path,inputData){
    var self = this;
    return new Ember.RSVP.Promise(function(resolve,reject){
                 Ember.$.ajax({
                     url :  self.buildUploadURL(path),
                     type : 'post',
                     data: JSON.stringify(inputData),
                     headers: self.get('headers'),
                     dataType : 'json'
                 }).done(function(data) {
                     resolve(data);
                 }).fail(function(error) {
                     reject(error);
                 });
              });
  },

  previewFromHDFS : function(previewFromHdfsData){
    console.log("preview from hdfs with info : ", previewFromHdfsData);
    return this.doPost("previewFromHdfs",previewFromHdfsData);
  },

  uploadFromHDFS : function(uploadFromHdfsData){
    console.log("upload from hdfs with info : ", uploadFromHdfsData);
    return this.doPost("uploadFromHDFS",uploadFromHdfsData);
  }
});
