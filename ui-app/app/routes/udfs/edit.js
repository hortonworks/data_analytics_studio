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
import UILoggerMixin from '../../mixins/ui-logger';
import commons from '../../mixins/commons';

export default Ember.Route.extend(UILoggerMixin, commons, {

  model(params){
    var udfId = params.udfId;
    return this.get('store').find('udf', udfId);
  },

  udfService: Ember.inject.service('udf'),

  resetUdfError(){
    this.get('controller').set('udfErrorObj.udfName', false);
    this.get('controller').set('udfErrorObj.udfClassName', false);
    this.get('controller').set('udfErrorObj.fileResource', false);
    this.get('controller').set('udfErrorObj.fileResourceName', false);
    this.get('controller').set('udfErrorObj.fileResourcePath', false);
  },

  validate(udfName, udfClassName, resourceName, resourcePath){

    if (Ember.isEmpty(udfName)) {
      this.get('controller').set('udfErrorObj.udfName', true);
      return false;
    } else if (Ember.isEmpty(udfClassName)) {
      this.get('controller').set('udfErrorObj.udfClassName', true);
      return false;
    } else if (Ember.isEmpty(resourceName)) {
      this.get('controller').set('udfErrorObj.fileResource', true);
      this.get('controller').set('udfErrorObj.fileResourceName', true);
      return false;
    } else if (Ember.isEmpty(resourcePath)) {
      this.get('controller').set('udfErrorObj.fileResource', true);
      this.get('controller').set('udfErrorObj.fileResourcePath', true);
      return false;
    }  else {
      return true;
    }
  },

  setupController(controller, model) {
    this._super(...arguments);

    controller.set('udfErrorObj', Ember.Object.create({
      udfName: false,
      udfClassName: false,
      fileResource: false,
      fileResourceName: false,
      fileResourcePath: false
    }));

    var fileResourceId = model.get('fileResource');

    this.store.findAll('file-resource').then((data) => {
      let fileResourceList = [];
      data.forEach(x => {
        let localFileResource = {'id': x.get('id'),
                                 'name': x.get('name'),
                                 'path': x.get('path'),
                                 'owner': x.get('owner')
                                };
        fileResourceList.push(localFileResource);
      });
      //fileResourceList.push({'name':'Add New File Resource', 'action':'addNewFileResource'});
      controller.set('fileResourceList', fileResourceList);

      let localSelectedFileResource =  fileResourceList.filterBy('id', fileResourceId);
      controller.set('selectedFileResource',localSelectedFileResource.get('firstObject'));
      controller.set('udfFileResourceName',localSelectedFileResource.get('firstObject').name);
      controller.set('udfFileResourcePath',localSelectedFileResource.get('firstObject').path);

    });

    controller.set('isAddingNewFileResource', false);
    controller.set('editUdfId', model.get('id'));
    controller.set('editUdfName', model.get('name'));
    controller.set('editUdfClassName', model.get('classname'));
    controller.set('editOwner', model.get('owner'));
    controller.set('editFileResource', model.get('fileResource'));

  },
  transitionToComposeRoute() {
    this.transitionTo('queries.query', 'saved');
  },
  actions: {
    handleResourceChange(filter){
      if(filter.action === "addNewFileResource"){
        this.get('controller').set('isAddingNewFileResource', true);
        //this.get('controller').set('selectedFileResource',null);
      }else {
        this.get('controller').set('selectedFileResource',filter);
        this.get('controller').set('isAddingNewFileResource', false);
      }
    },

    saveUDf(name, classname, udfid, udfFileResourceName, udfFileResourcePath){
      let self = this;
      this.logGA('UDF_EDIT');
      this.resetUdfError();
      if (this.validate(name, classname, udfFileResourceName, udfFileResourcePath)) {

        if (!Ember.isEmpty(this.get('controller').get('selectedFileResource'))) {

          this.get('store').findRecord('udf', udfid).then(function (resultUdf) {
            resultUdf.set('name', name);
            resultUdf.set('classname', classname);
            resultUdf.set('fileResource', self.get('controller').get('selectedFileResource').id );
            resultUdf.save();

            Ember.run.later(()=>{
                  self.transitionToComposeRoute();
                }, 2000);

          });
        } else {

          let resourcePayload = {"name": udfFileResourceName, "path": udfFileResourcePath};

          this.get('udfService').savefileResource(resourcePayload)
            .then((data) => {
              console.log('fileResource is', data.fileResource.id);
              self.get('store').findRecord('udf', udfid).then(function (resultUdf) {

                resultUdf.set('name', name);
                resultUdf.set('classname', classname);
                resultUdf.set('fileResource', data.fileResource.id);
                resultUdf.save();

                Ember.run.later(()=>{
                  self.transitionToComposeRoute();
                }, 2000);

              });
            }, (error) => {
              console.log("Error encountered", error);
            });
        }

      } else {
        console.log('validation error');
      }
      this.get('controller').set('isAddingNewFileResource', false);
    },

    cancelEditUdf(){
      this.transitionToComposeRoute();
    },

    goToAddNewFileResource(){
      /* TODO:: set the already selected file resource state */
      this.get('controller').set('isAddingNewFileResource',false);
    }

  }
});
