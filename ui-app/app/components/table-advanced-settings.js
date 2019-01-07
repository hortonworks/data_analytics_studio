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
import fileFormats from '../configs/file-format';
import Helpers from '../configs/helpers';


export default Ember.Component.extend({

  classNames: ['create-table-advanced-wrap'],
  showLocationInput: false,
  showFileFormatInput: false,
  showRowFormatInput: false,
  shouldAddBuckets: false,
  errors: [],
  editMode: false,
  disableTransactionInput: false,
  disableNumBucketsInput: false,

  settings: {},

  errorsObserver: Ember.observer('errors.[]', function() {
    let numBucketsError = this.get('errors').findBy('type', 'numBuckets');
    if(!Ember.isEmpty(numBucketsError)) {
      this.set('hasNumBucketError', true);
      this.set('numBucketErrorText', numBucketsError.error);
    }
  }).on('init'),


  fileFormats: Ember.copy(fileFormats),
  terminationChars: Ember.computed(function () {
    return Helpers.getAllTerminationCharacters();
  }),

  didReceiveAttrs() {
    if (!Ember.isEmpty(this.get('settings.location'))) {
      this.set('showLocationInput', true);
    }
    if (!Ember.isEmpty(this.get('settings.fileFormat'))) {
      this.set('showFileFormatInput', true);
      let currentFileFormat = this.get('fileFormats').findBy('name', this.get('settings.fileFormat.type'));
      this.set('selectedFileFormat', currentFileFormat);
      this.set('customFileFormat', currentFileFormat.custom);
    } else {
      let defaultFileFormat = this.get('fileFormats').findBy('default', true);
      this.set('settings.fileFormat', {});
      this.set('settings.fileFormat.type', defaultFileFormat.name);
    }
    if (!Ember.isEmpty(this.get('settings.rowFormat'))) {
      this.set('showRowFormatInput', true);
      this.set('selectedFieldTerminator', this.get('settings.rowFormat.fieldTerminatedBy'));
      this.set('selectedLinesTerminator', this.get('settings.rowFormat.linesTerminatedBy'));
      this.set('selectedNullDefinition', this.get('settings.rowFormat.nullDefinedAs'));
      this.set('selectedEscapeDefinition', this.get('settings.rowFormat.escapeDefinedAs'));
    }
    if(!Ember.isEmpty(this.get('settings.transactional')) && this.get('settings.transactional') && this.get('editMode')) {
      this.set('disableTransactionInput', true);
    }

    if(!Ember.isEmpty(this.get('settings.numBuckets')) && this.get('settings.numBuckets') && this.get('editMode')) {
      this.set('disableNumBucketsInput', true);
    }
  },

  locationInputObserver: Ember.observer('showLocationInput', function () {
    if (!this.get('showLocationInput')) {
      this.set('settings.location');
    }
  }),

  fileFormatInputObserver: Ember.observer('showFileFormatInput', function () {
    if (!this.get('showFileFormatInput')) {
      this.set('settings.fileFormat');
    } else {
      this.set('selectedFileFormat', this.get('fileFormats').findBy('default', true));
    }
  }),

  rowFormatInputObserver: Ember.observer('showRowFormatInput', function () {
    if (!this.get('showRowFormatInput')) {
      this.send('clearFieldTerminator');
      this.send('clearLinesTerminator');
      this.send('clearNullDefinition');
      this.send('clearEscapeDefinition');
      this.set('settings.rowFormat');
    } else {
      this.set('settings.rowFormat', {});
    }
  }),

  actions: {

    closeHdfsModal() {
      this.set('showDirectoryViewer', false);
    },

    hdfsPathSelected(path) {
      this.set('settings.location', path);
      this.set('showDirectoryViewer', false);
    },

    toggleDirectoryViewer() {
      this.set('showDirectoryViewer', true);
    },

    toggleLocation() {
      this.toggleProperty('showLocationInput');
    },

    toggleFileFormat() {
      this.toggleProperty('showFileFormatInput');
    },

    toggleRowFormat() {
      this.toggleProperty('showRowFormatInput');
    },

    fileFormatSelected(format) {
      this.set('settings.fileFormat.type', format.name);
      this.set('selectedFileFormat', format);
      this.set('customFileFormat', format.custom);
    },

    fieldTerminatorSelected(terminator) {
      this.set('settings.rowFormat.fieldTerminatedBy', terminator);
      this.set('selectedFieldTerminator', terminator);
    },
    clearFieldTerminator() {
      this.set('settings.rowFormat.fieldTerminatedBy');
      this.set('selectedFieldTerminator');
    },

    linesTerminatorSelected(terminator) {
      this.set('settings.rowFormat.linesTerminatedBy', terminator);
      this.set('selectedLinesTerminator', terminator);
    },
    clearLinesTerminator() {
      this.set('settings.rowFormat.linesTerminatedBy');
      this.set('selectedLinesTerminator');
    },

    nullDefinedAsSelected(terminator) {
      this.set('settings.rowFormat.nullDefinedAs', terminator);
      this.set('selectedNullDefinition', terminator);
    },
    clearNullDefinition() {
      this.set('settings.rowFormat.nullDefinedAs');
      this.set('selectedNullDefinition');
    },

    escapeDefinedAsSelected(terminator) {
      this.set('settings.rowFormat.escapeDefinedAs', terminator);
      this.set('selectedEscapeDefinition', terminator);
    },
    clearEscapeDefinition() {
      this.set('settings.rowFormat.escapeDefinedAs');
      this.set('selectedEscapeDefinition');
    },
  }
});
