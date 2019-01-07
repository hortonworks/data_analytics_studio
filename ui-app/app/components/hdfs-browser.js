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
import HdfsViewerConfig from '../utils/hdfsviewer';
export default Ember.Component.extend({
  config: HdfsViewerConfig.create(),
//  uploaderService : Ember.inject.service('hdfs-file-uploader'),
//  userInfo : Ember.inject.service('workspace-manager'),
  initialize:function(){
    var self=this;
    self.$("#filediv").modal("show");
    self.$("#filediv").on('hidden.bs.modal', function (e) {
      self.sendAction('closeWorkflowSubmitConfigs');
      self.sendAction("closeFileBrowser");
    });

  setInterval(()=>{
    this.$(document).tooltip("destroy");
    this.$(document).tooltip({
      show: {
        delay: 500
      },
      tooltipClass: 'generic-tooltip'
    });
  }, 5000);

  }.on('didInsertElement'),
  setUserData : function() {
//    this.set("homeDirectory", "/user/"+this.get("userInfo").getUserName());
//    this.set("selectedPath", "/user/"+this.get("userInfo").getUserName());
//    this.set("filePath", "/user/"+this.get("userInfo").getUserName());
  }.on("init"),
  selectFileType: "all",//can be all/file/folder
  selectedPath:"",
  isDirectory:false,
  callback: null,
  alertMessage:null,
  alertDetails:null,
  alertType:null,
  uploadSelected: false,
  isFilePathInvalid: Ember.computed('selectedPath',function() {
    return this.get("selectedPath").indexOf("<")>-1;
  }),
  showNotification(data){
    if (!data){
      return;
    }
    if (data.type==="success"){
      this.set("alertType","success");
    }
    if (data.type==="error"){
      this.set("alertType","danger");
    }
    this.set("alertDetails",data.details);
    this.set("alertMessage",data.message);
  },
  isUpdated : function(){
    if(this.get('showUploadSuccess')){
      this.$('#success-alert').fadeOut(5000, ()=>{
        this.set("showUploadSuccess", false);
      });
    }
  }.on('didUpdate'),
  actions: {
    viewerError(error) {
      if (error.responseJSON && error.responseJSON.message && error.responseJSON.message.includes("Permission")) {
        this.showNotification({"type": "error", "message": "Permission denied"});
      }
    },
    createFolder(){
      var self=this;
      var $elem=this.$('input[name="selectedPath"]');
      //$elem.val($elem.val()+"/");
      var folderHint="<enter folder here>";
      this.set("selectedPath",this.get("selectedPath")+"/"+folderHint);
      setTimeout(function(){
        $elem[0].selectionStart = $elem[0].selectionEnd = self.get("selectedPath").length-folderHint.length;
      },10);

      $elem.focus();

    },
    viewerSelectedPath(data) {
      this.set("selectedPath",data.path);
      this.set("filePath",data.path);
      this.set("isDirectory",data.isDirectory);
      this.set("alertMessage",null);
    },
    selectFile(){
      if (this.get("selectedPath")===""){
        this.showNotification( {"type": "error", "message": "Please fill the settings value"});
        return false;
      }
      if (this.get("selectFileType")==="folder" && !this.get("isDirectory")){
        this.showNotification( {"type": "error", "message": "Only folders can be selected"});
        return false;
      }
      this.set("filePath",this.get("selectedPath"));
      this.$("#filediv").modal("hide");
    },
    uploadSelect(){
      this.set("uploadSelected",true);
    },

    closeUpload(){
      this.set("uploadSelected",false);
    },
    uploadSuccess(e){
      this.get('uploaderService').trigger('uploadSuccess');
      this.set('uploadSelected', false);
      this.set('showUploadSuccess', true);
    },
    uploadFailure(textStatus,errorThrown){
      this.showNotification({
        "type": "error",
        "message": "Upload Failed",
        "details":textStatus,
        "errorThrown":errorThrown
      });
    },
    uploadProgress(e){
    },
    uploadValidation(e){
      this.showNotification({
        "type": "error",
        "message": "Upload Failed",
        "details":e
      });
    }
  }
});
