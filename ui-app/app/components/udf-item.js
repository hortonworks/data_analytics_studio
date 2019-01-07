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
import UILoggerMixin from '../mixins/ui-logger';

export default Ember.Component.extend(UILoggerMixin, {

  store: Ember.inject.service(),

  udfService: Ember.inject.service('udf'),

  tagName: '',
  expanded: false,
  expandedEdit: false,
  showDeleteUdfModal: false,
  expandedValue: null,
  udfId: null,
  editUdfId: Ember.computed('udf', function () {
    return this.get('udf.id');
  }),
  editUdfName: Ember.computed('udf', function () {
    return this.get('udf.name');
  }),
  editUdfClassName: Ember.computed('udf', function () {
    return this.get('udf.classname');
  }),
  editOwner: Ember.computed('udf', function () {
    return this.get('udf.owner');
  }),
  editFileResource: Ember.computed('udf', function () {
    return this.get('udf.fileResource');
  }),
  fileResourceList:[],
  selectedFileResource: null,
  isAddingNewFileResource: false,

  validate(udfName, udfClassName, resourceName, resourcePath){
    if (Ember.isEmpty(udfName)) {
      this.get('logger').danger('UDF Name can not be empty.');
      return false;
    }

    if (Ember.isEmpty(udfClassName)) {
      this.get('logger').danger('UDF Class Name can not be empty.');
      return false;
    }

    if (Ember.isEmpty(resourceName) || Ember.isEmpty(resourcePath)) {
      this.get('logger').danger('File Resource can not be empty.');
      return false;
    }
    return true;
  },

  actions: {
    toggleExpandUdf(fileResourceId) {

      if(this.get('expanded')) {
        this.set('expanded', false);
      } else {
        this.set('expanded', true);
        this.set('valueLoading', true);

        this.get('store').find('fileResource', fileResourceId).then((data) => {
          this.set('udfFileResourceName', data.get('name'));
          this.set('udfFileResourcePath', data.get('path'));
        });
      }
    },


    showEditUdf(udfId, fileResourceId){

      if(this.get('expandedEdit')) {
        this.set('expandedEdit', false);
      } else {
        this.set('expandedEdit', true);
        this.set('valueLoading', true);

        this.get('store').findAll('file-resource').then((data) => {
          let fileResourceList = [];
          data.forEach(x => {
            let localFileResource = {'id': x.get('id'),
              'name': x.get('name'),
              'path': x.get('path'),
              'owner': x.get('owner')
            };
            fileResourceList.push(localFileResource);
          });

          fileResourceList.filterBy('id', fileResourceId).map((data) => {
            this.set('udfFileResourceName', data.name);
            this.set('udfFileResourcePath', data.path);

                    this.get('store').find('udf', udfId).then((data) => {
                        this.set('editUdfId', udfId);
                        this.set('editUdfName', data.get('name'));
                        this.set('editUdfClassName', data.get('classname'));
                        this.set('editOwner', data.get('owner'));
                      });
          });
        });
        this.send('setFileResource', fileResourceId);
      }
    },

    cancelEditUdf(){
      this.set('expandedEdit', false);
      this.set('isAddingNewFileResource', false);
    },

    saveUDf(name, classname, udfid, udfFileResourceName, udfFileResourcePath){
      let self = this;
      if (this.validate(name, classname, udfFileResourceName, udfFileResourcePath)) {
        if (!Ember.isEmpty(this.get('selectedFileResource'))) {
          this.get('store').findRecord('udf', udfid).then(function (resultUdf) {
            resultUdf.set('name', name);
            resultUdf.set('classname', classname);
            resultUdf.set('fileResource', self.get('selectedFileResource').id);
            resultUdf.save();
            self.set('expandedEdit', false);
          });
        }
        else {

          let resourcePayload = {"name": udfFileResourceName, "path": udfFileResourcePath};

          this.get('udfService').savefileResource(resourcePayload)
            .then((data) => {
              console.log('fileResource is', data.fileResource.id);
              self.get('store').findRecord('udf', udfid).then(function (resultUdf) {

                resultUdf.set('name', name);
                resultUdf.set('classname', classname);
                resultUdf.set('fileResource', data.fileResource.id);
                resultUdf.save();
                self.set('expandedEdit', false);
              });
            }, (error) => {
              console.log("Error encountered", error);
            });
        }
      }
      this.set('isAddingNewFileResource', false);
    },

    showRemoveUdfModal(udfId){
      console.log('udfId',udfId);
      this.set('showDeleteUdfModal', true);
      this.set('udfId', udfId);
    },

    removeUdf(){
      var self = this;
      let record = this.get('store').peekRecord('udf', this.get('udfId') );
      if(record){
        record.destroyRecord().then(function(){
          self.send('cancelUdf');
          self.sendAction('refreshUdfList');
      })}
    },

    cancelUdf(){
      this.set('showDeleteUdfModal', false);
    },

    handleResourceChange(filter){
      if(filter.action === "addNewFileResource"){
        this.get('controller').set('isAddingNewFileResource', true);
        this.set('selectedFileResource',null);
      }else {
        this.set('selectedFileResource',filter);
        this.get('controller').set('isAddingNewFileResource', false);
      }
    },

    setFileResource(fileResourceId){
      this.get('store').findAll('file-resource').then((data) => {
        let fileResourceList = [];
        data.forEach(x => {
          let localFileResource = {'id': x.get('id'),
            'name': x.get('name'),
            'path': x.get('path'),
            'owner': x.get('owner')
          };
          fileResourceList.push(localFileResource);
        });

        let localSelectedFileResource =  fileResourceList.filterBy('id', fileResourceId);
        this.set('selectedFileResource',localSelectedFileResource.get('firstObject'));
      });

    }
  }

});
