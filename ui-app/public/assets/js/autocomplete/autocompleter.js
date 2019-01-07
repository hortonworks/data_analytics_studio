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

var Autocompleter = (function () {

  /**
   * @param {Object} options {object}
   * @param options.snippet
   * @param options.user
   * @param options.optEnabled
   * @param {Number} options.timeout
   * @constructor
   */
  function Autocompleter(options) {
    var self = this;
    self.snippet = options.snippet;
    self.timeout = options.timeout;
    
    self.topTables = {};

    var initializeAutocompleter = function () {
      if (self.snippet.isSqlDialect()) {
        self.autocompleter = new SqlAutocompleter2({
          snippet: self.snippet,
          timeout: self.timeout
        });
      } else {
        var hdfsAutocompleter = new HdfsAutocompleter({
          user: options.user,
          snippet: options.snippet,
          timeout: options.timeout
        });
        if (self.snippet.isSqlDialect()) {
          self.autocompleter = new SqlAutocompleter({
            hdfsAutocompleter: hdfsAutocompleter,
            snippet: options.snippet,
            oldEditor: options.oldEditor,
            optEnabled: options.optEnabled,
            timeout: self.timeout
          })
        } else {
          self.autocompleter = hdfsAutocompleter;
        }
      }
    };
    self.snippet.type.subscribe(function () {
      initializeAutocompleter();
    });
    initializeAutocompleter();
  }

  // TODO: See why we need this one.
  Autocompleter.prototype.initializeAutocompleter = function () {
    var self = this;
  };

  // ACE Format for autocompleter
  Autocompleter.prototype.getCompletions = function (editor, session, pos, prefix, callback) {
    var self = this;
    if (! self.autocompleter) {
      return;
    }

    var before = editor.getTextBeforeCursor();
    var after = editor.getTextAfterCursor(";");

    try {
      self.autocomplete(before, after, function(result) {
        callback(null, result);
      }, editor);
    } catch (err) {
      editor.hideSpinner();
    }
  };

  Autocompleter.prototype.getDocTooltip = function (item) {
    var self = this;
    return self.autocompleter.getDocTooltip(item);
  };


  Autocompleter.prototype.autocomplete = function(beforeCursor, afterCursor, callback, editor) {
    var self = this;
    if (self.autocompleter) {
      self.autocompleter.autocomplete(beforeCursor, afterCursor, callback, editor);
    }
  };

  return Autocompleter;
})();