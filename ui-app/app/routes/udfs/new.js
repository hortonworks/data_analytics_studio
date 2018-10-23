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

  breadCrumb: {
   'title': 'New',
   'path': 'udfs',
   'linkable': true
  },
  beforeModel(){

  },

  resetUdfError(){
    this.get('controller').set('udfErrorObj.udfName', false);
    this.get('controller').set('udfErrorObj.udfClassName', false);
    this.get('controller').set('udfErrorObj.fileResource', false);
    this.get('controller').set('udfErrorObj.fileResourceName', false);
    this.get('controller').set('udfErrorObj.fileResourcePath', false);
  },

  validateUDF(udfName, udfClassName){
    if (Ember.isEmpty(udfName)) {
      this.get('controller').set('udfErrorObj.udfName', true);
      return false;
    } else if (Ember.isEmpty(udfClassName)) {
      this.get('controller').set('udfErrorObj.udfClassName', true);
      return false;
    } else{
      return true;
    }
  },

  validateFileResource(resourceName, resourcePath){

    if (Ember.isEmpty(resourceName) ) {
      this.get('controller').set('udfErrorObj.fileResource', true);
      this.get('controller').set('udfErrorObj.fileResourceName', true);
      return false;
    } else if(Ember.isEmpty(resourcePath) ){
      this.get('controller').set('udfErrorObj.fileResource', true);
      this.get('controller').set('udfErrorObj.fileResourcePath', true);
      return false;
    } else {
      return true;
    }
  },

  udf: Ember.inject.service(),
  store: Ember.inject.service(),

  setupController(controller, model) {
    this._super(...arguments);

    controller.set('udfErrorObj', Ember.Object.create({
      udfName: false,
      udfClassName: false,
      fileResource: false,
      fileResourceName: false,
      fileResourcePath: false
    }));

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
      fileResourceList.push({'name':'Add New File Resource', 'action':'addNewFileResource'});
      controller.set('fileResourceList', fileResourceList);
    });

    controller.set('isAddingNewFileResource', false);
    controller.set('selectedFileResource',null);
    controller.set('resourceId', null);

  },
  transitionToComposeRoute() {
    this.transitionTo('queries.query', 'saved');
  },
  actions: {

    saveUDf(resourceName, resourcePath, udfName, udfClassName){
      this.logGA('UDF_CREATE');
      this.get('controller').set('resourceName',resourceName);
      this.get('controller').set('resourcePath', resourcePath);
      this.get('controller').set('udfName', udfName);
      this.get('controller').set('udfClassName', udfClassName);

      this.resetUdfError();

      if(!Ember.isEmpty( this.get('controller').get('resourceId'))){

        if (this.validateUDF(udfName, udfClassName)) {

          let newUDF = this.get('store').createRecord('udf',
            {
              name: udfName,
              classname: udfClassName,
              fileResource: this.get('controller').get('resourceId')
            });

          newUDF.save().then((data) => {
            console.log('udf saved');

            this.get('store').findAll('udf').then((data) => {
              let udfList = [];
              data.forEach(x => {
                let localUdf = {
                  'id': x.get('id'),
                  'name': x.get('name'),
                  'classname': x.get('classname'),
                  'fileResource': x.get('fileResource'),
                  'owner': x.get('owner')
                };
                udfList.pushObject(localUdf);
              });

              this.controllerFor('udfs').set('udflist', udfList);
              this.transitionToComposeRoute();
            });

          });

        }

      } else {

        let resourcePayload = {"name":resourceName,"path":resourcePath};

        if (this.validateUDF(udfName, udfClassName) && this.validateFileResource(resourceName, resourcePath)) {

          var newFileResource = this.get('store').createRecord('file-resource', {name:resourceName, path:resourcePath});

          newFileResource.save().then((data) => {
              console.log('fileResource is', data.get('id'));
              let newUDF = this.get('store').createRecord('udf',
                {name:udfName,
                  classname:udfClassName,
                  fileResource: data.get('id')
                });

              newUDF.save().then((data) => {
                  console.log('udf saved');

                  this.get('store').findAll('udf').then((data) => {
                    let udfList = [];
                    data.forEach(x => {
                      let localUdf = {
                        'id': x.get('id'),
                        'name': x.get('name'),
                        'classname': x.get('classname'),
                        'fileResource': x.get('fileResource'),
                        'owner': x.get('owner')
                      };
                      udfList.pushObject(localUdf);
                    });

                    this.controllerFor('udfs').set('udflist',udfList);
                    this.transitionToComposeRoute();
                  });
                })
                .catch((error) => {
                  this.get('logger').danger('Failed to create UDF.', this.extractError(error));
                  this.transitionToComposeRoute();

                });
            })
            .catch((error) => {
              this.get('logger').danger('Failed to create File Resource.', this.extractError(error));
              this.transitionToComposeRoute();
            });

        }

      }
    },

    cancelSaveUDf(){
      this.get('controller').set('resourceName','');
      this.get('controller').set('resourcePath','');
      this.get('controller').set('udfName','');
      this.get('controller').set('udfClassName','');

      this.transitionToComposeRoute();
    },

    handleFileResourceChange(filter){
      console.log('filter', filter);
      if(filter.action === "addNewFileResource"){
        this.get('controller').set('isAddingNewFileResource', true);
        this.get('controller').set('resourceName','');
        this.get('controller').set('resourcePath','');
        this.get('controller').set('resourceId', null);
        this.get('controller').set('selectedFileResource',null);

      }else {
        this.get('controller').set('resourceId',filter.id);
        this.get('controller').set('selectedFileResource',filter);
        this.get('controller').set('isAddingNewFileResource', false);
      }
    },
  }

});
