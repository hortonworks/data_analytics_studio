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

  tagName: "query-editor",
  readOnly: false,
  /*

     event.keyCode != 13 &&         Enter - do not open autocomplete list just after item has been selected in it
     event.keyCode != 16 &&     //not for shift
     event.keyCode != 17 &&     //not for ctrl
     event.keyCode != 18 &&     //not for alt
     event.keyCode != 60 &&     //not for < or >
     event.keyCode != 8 &&      //not for the backspace key
     event.keyCode != 9  &&     //not for the tab key, keyup down left and right
     event.keyCode != 186       //not for the ; key

  */
  excludeKeycodeCombination : [8, 9, 13, 16, 17, 18, 27, 37, 38, 39, 40, 60],
  store: Ember.inject.service(),
  queryService: Ember.inject.service('query'),
  selectedDb: Ember.computed('selectedMultiDb', function() {
    let name;
    this.get('selectedMultiDb').forEach((item => {
      name = item;
    }));
    return name;
  }),
  aggregateFunctions: ["count()", "sum()", "avg()", "min()", "max()", "variance()", "var_pop()", "var_samp()", "stddev_pop()", "stddev_samp()",
  "covar_pop()", "covar_samp()", "corr()", "percentile()", "percentile_approx()", "histogram_numeric()", "collect_set()", "cast()", "coalesce()"],
  _initializeEditor: function() {

    var editor,
      updateSize,
      self = this;
    this.browserDetection();
    updateSize = function () {
      editor.setSize(self.$(this).width(), self.$(this).height());
      editor.refresh();
    };
    let codeMirrorConfig = {
                                 mode: 'text/x-hive',
                                 indentWithTabs: true,
                                 closeOnUnfocus: false,
                                 smartIndent: true,
                                 lineNumbers: true,
                                 matchBrackets : true,
                                 autofocus: true,
                                 readOnly: self.get('readOnly')
                           };
    if(!this.get('readOnly')) {
     codeMirrorConfig.extraKeys = {'Ctrl-Space': 'autocomplete'};
    }

    this.set('editor', CodeMirror.fromTextArea(document.getElementById('code-mirror'), codeMirrorConfig));

    var orig = CodeMirror.hint.sql;
    CodeMirror.hint.sql = function(cm) {

      var inner = orig(cm) || {from: cm.getCursor(), to: cm.getCursor(), list: []};
      var lastWord = cm.getValue().split(' ').pop();
      try {
          inner.list = [];
          inner.list = self.formCustomSuggestions(self.get('queryService').extractTableNamesAndColumns(self.get("selectedMultiDb")), lastWord, cm, self.get("alldatabases"));//.concat(inner.list);
      } catch(e) {}
      return inner;
    };
    if(!this.get('readOnly')) {
        this.get('editor').on('keyup', function(editor, event){
            let key = event.which || event.keyCode; // keyCode detection
            let ctrl = event.ctrlKey ? event.ctrlKey : ((key === 17) ? true : false), metaKey = event.key; // ctrl detection
            if ( (key == 86 && ctrl) || (metaKey === "Meta") ) {
              return;
            }
            if(self.get('excludeKeycodeCombination').indexOf(event.keyCode) === -1) {
              CodeMirror.commands.autocomplete(editor, null, {completeSingle: false});
            }
        });
    }

    CodeMirror.commands.autocomplete = function (cm) {

      var autocompleteOptions = { closeOnUnfocus: false, completeSingle: false };
      Ember.run.later(() => {
        CodeMirror.showHint(cm, "", autocompleteOptions);
      }, 5);

    };

    editor = this.get('editor');

    editor.on('cursorActivity', function () {
      self.set('highlightedText', editor.getSelections());
    });
    editor.setValue(this.get('query') || '');

    editor.on('change', function (instance) {
      Ember.run(function () {
        if(instance) {
            self.set('query', instance.getValue());
        }
      });
    });

    this.$('.CodeMirror').resizable({
      handles: 's',

      resize: function () {
        Ember.run.debounce(this, updateSize, 150);
      }
    }).find('.ui-resizable-s').addClass('grip fa fa-reorder');


  }.on('didInsertElement'),

  updateValue: function () {
    try {
      var query = this.get('query');
      var editor = this.get('editor');

      var isFinalExplainQuery = (query.toUpperCase().trim().indexOf('EXPLAIN') > -1);
      var editorQuery = editor.getValue();

      if (editor.getValue() !== query) {
        if(isFinalExplainQuery){
          editor.setValue(editorQuery || '');
        }else {
          editor.setValue(query || '');
        }
      }
      this.sendAction('updateQuery', query);
    } catch(e) { }
  }.observes('query'),
  insertHeadersToSuggestions(tableColList, headerTitle) {
    tableColList.push({"displayText" : headerTitle, "className" : "suggestion-headings", "text": ""});
    return tableColList;
  },
  browserDetection() {
      //Check if browser is IE, chrome, safari
      if ((navigator.userAgent.search("MSIE") >= 0) || (navigator.userAgent.search("Chrome") >= 0) || (navigator.userAgent.search("Safari") >= 0 && navigator.userAgent.search("Chrome")< 0)) {
          this.get('excludeKeycodeCombination').push(186);
      }
      //Check if browser is Firefox, opera
      else if ((navigator.userAgent.search("Firefox") >= 0) || (navigator.userAgent.search("Opera") >= 0)) {
          this.get('excludeKeycodeCombination').push(59);
      }
  },
  extractKeywords(suggestions, dbTables, cm) {
     let keyWords = [];
     for(var p=0; p<suggestions.suggestKeywords.length; p++) {
       keyWords.push(suggestions.suggestKeywords[p].value);
     }
     if(keyWords.length === 1 && keyWords[0].toLowerCase() === "from") {
       keyWords = keyWords.concat(this.formTables(dbTables, cm, "FROM "));
     }
     return keyWords;
  },
  extractAlias(table, suggestions, index) {
    if(table.alias) {
        return table.alias;
    } else {
        let suggestion = this.findAliasFromMeta(suggestions), alias = "";
        suggestion.forEach(function(item, i){
            if(table.identifierChain[0].name === (item.alias || item.identifierChain[0].name)) {
               alias = item.alias;
            }
        })
        return alias;
    }
  },
  extractColumnsFromTables(suggestions, subQueries, aliasToTable, dbTables, cm, tableColList, aliasOfQuery) {
     for(var p=0; p<suggestions.suggestColumns.tables.length; p++) {
      if(!$.isEmptyObject(subQueries)) {
        tableColList = this.insertHeadersToSuggestions(tableColList, "Columns");
      }
      let subQueriesItem = subQueries[suggestions.suggestColumns.tables[p].identifierChain[0].subQuery+"."] || subQueries[aliasOfQuery.aliasList[suggestions.suggestColumns.tables[p].identifierChain[0].name]+"."];

      if(!subQueriesItem) {
          subQueriesItem = subQueries[this.extractAlias(suggestions.suggestColumns.tables[p], suggestions, p)+"."];
          aliasToTable[suggestions.suggestColumns.tables[p].alias+"."] = suggestions.suggestColumns.tables[p].identifierChain[0].name;
      }
      if(subQueriesItem){
        tableColList = tableColList.concat(subQueriesItem.combinations);
      } else {
        let tableIdentifier = suggestions.suggestColumns.tables[p].identifierChain[0].name?suggestions.suggestColumns.tables[p].identifierChain[0].name:"";
        tableColList = this.formTableColumns(dbTables, cm, `${tableIdentifier}.`, tableIdentifier).concat(tableColList);
        tableColList = this.formTableColumns(dbTables, cm, "", tableIdentifier).concat(tableColList);
      }
     }
     return tableColList;
  },
  extractIdentifiers(dbTables, cm, suggestions, aliasToTable, tableColList) {
    for(var p=0; p<suggestions.suggestIdentifiers.length; p++) {
      tableColList.push(suggestions.suggestIdentifiers[p].name);
      if(aliasToTable[suggestions.suggestIdentifiers[p].name]) {
        tableColList = tableColList.concat(this.formTableColumns(dbTables, cm, suggestions.suggestIdentifiers[p].name, aliasToTable[suggestions.suggestIdentifiers[p].name]));
      }
    }
    return tableColList;
  },
  extractUDF(udfList) {
    if(this.get('allUDFList') && this.get("showUDF")){
        this.get('allUDFList').forEach(x => {
          udfList.push({"text":x.name+"()", "className":"udf-suggestion"});
        });
    }
    return udfList;
  },
  extactSuggestion(cm) {
      let {leftPortion, rightPortion} = this.splitQueryText(cm);
      return sqlAutocompleteParser.parseSql(leftPortion, rightPortion, "hive", false);
  },
  splitQueryText(cm) {
    let queryText = cm.getValue(), cursor = cm.getCursor(), {ch:cursorPos, line:currentLine} = cursor, currentLineTxt = "";
    let noOfQueryLines = queryText.split(/\r\n|\r|\n/).length;
    let leftPortionMultiLine = "", rightPortionMultiLine = "", currentLineLeftPortion = "", currentLineRightPortion = "" ;
    for(let i=0; i< noOfQueryLines; i++) {
        if(currentLine == i) {
            currentLineTxt = cm.getLine(i);
            currentLineLeftPortion = currentLineTxt.slice(0, cursorPos), currentLineRightPortion = currentLineTxt.slice(cursorPos, currentLineTxt.length);
        } else if(currentLine > i) {
            leftPortionMultiLine += ` ${cm.getLine(i)} `;
        } else {
            rightPortionMultiLine += ` ${cm.getLine(i)} `;
        }
    }
    let leftPortion = leftPortionMultiLine.split(";").reverse()[0] + currentLineLeftPortion, rightPortion = currentLineRightPortion + rightPortionMultiLine.split(";")[0];
    return {leftPortion, rightPortion};
  },
  formCustomSuggestions(dbTables, lastWord, cm, databases) {
    var tableColList = [], udfList = [], queryValue = cm.getValue();

    udfList = this.extractUDF(udfList);
    if(udfList.length) {
      tableColList = this.insertHeadersToSuggestions(tableColList, "UDF");
      tableColList = tableColList.concat(udfList);
    }

    var lastToken = queryValue.slice(0, cm.getCursor().ch).trim().split(" ").pop();
    let nextTokens = queryValue.slice(cm.getCursor().ch, queryValue.length).trim().split(" ");

    var suggestions = this.extactSuggestion(cm);
    let subQueries = this.parseSubQueries(suggestions, dbTables, cm);
    let aliasOfQuery = this.parseAlias(suggestions, dbTables, cm), aliasToTable = {};
    subQueries = $.extend(subQueries, aliasOfQuery.alias);

    if(suggestions.suggestKeywords) {
     let keyWords = this.extractKeywords(suggestions, dbTables, cm);
     if(keyWords.length) {
       tableColList = this.insertHeadersToSuggestions(tableColList, "Keywords");
     }
     tableColList = tableColList.concat(keyWords);
    } else if(suggestions.suggestDatabases && suggestions.suggestDatabases.appendDot === true && suggestions.suggestTables){
     let tableList = this.formTables(dbTables, cm, "");
     if(tableList.length) {
       tableColList = this.insertHeadersToSuggestions(tableColList, "Tables");
     }
     tableColList = tableColList.concat(tableList);
    }
    if(suggestions.suggestColumns) {
      tableColList = this.extractColumnsFromTables(suggestions, subQueries, aliasToTable, dbTables, cm, tableColList, aliasOfQuery);
    }
    if(suggestions.suggestDatabases && $.isEmptyObject(suggestions.suggestDatabases)) {
      tableColList = this.formDatabases(tableColList, databases);
    }
    if(suggestions.suggestIdentifiers){
      tableColList = this.extractIdentifiers(dbTables, cm, suggestions, aliasToTable, tableColList);
      tableColList = this.insertHeadersToSuggestions(tableColList, "Keywords");
    }
    if(suggestions.suggestColumnAliases) {
      tableColList = this.extractColumnAliases(suggestions, tableColList);
    }
    tableColList = this.addAggregateFunctions(suggestions, tableColList);
    tableColList = this.removeDuplicateSuggestions(tableColList);
    return this.filterList(cm, tableColList);
  },
  extractColumnAliases(suggestions, tableColList) {
    let colAliasList = [], aliases = suggestions.suggestColumnAliases;
    for(let i of aliases) {
      colAliasList.push(i.name)
    }
    return [...tableColList, ...colAliasList];
  },
  addAggregateFunctions(suggestions, tableColList) {
    if(suggestions.suggestAggregateFunctions) {
      tableColList = this.insertHeadersToSuggestions(tableColList, "Aggregate functions");
      return [...tableColList, ...this.get("aggregateFunctions")];
    }
    return tableColList;
  },
  extractFilterText(cm) {
      let cursor = cm.getCursor(), {ch:cursorPos, line:currentLine} = cursor;
      let filterTxt = cm.getLine(currentLine).substr(0, cm.getCursor().ch).split(" ").pop(), index, indexOfCommaOperator;
      if(filterTxt.split(",").length > 1) {
        filterTxt = filterTxt.split(",").pop();
      }
      if(filterTxt.endsWith(",")){
          filterTxt = "";
      }
      index  = filterTxt.lastIndexOf("(");
      indexOfCommaOperator = filterTxt.indexOf(",")
      if(indexOfCommaOperator > -1) {
        ({index } = this.checkMultipleColumns(filterTxt, indexOfCommaOperator));
      }
      if(index >= 0) {
        filterTxt = filterTxt.substr(index+1, filterTxt.length);
      }
      return filterTxt;
  },
  checkMultipleColumns(filterTxt, index) {
      let filterTokens = filterTxt.split(","), len = filterTokens.length;
      return {filterTxt:filterTokens[len-1], index};
  },
  filterList(cm, tableColList){
   var filter = this.extractFilterText(cm);
   if(filter == "(") {
        return tableColList;
   }
   if(!filter){
    return tableColList;
   }
   return tableColList.filter(function(item, index, enumerable){
     if(item.hasOwnProperty("text")){
       return item.text.toLowerCase().indexOf(filter.toLowerCase)>-1;
     }
     return item.toLowerCase().indexOf(filter.toLowerCase())>-1;
   });

  },
  removeDuplicateSuggestions(tableColList) {
      return tableColList.filter(function(item, pos) {
         return tableColList.indexOf(item) == pos;
      });
  },
  parseSubQueries(suggestions, dbTables, cm){
   let subQueries = suggestions.subQueries, alias = {};
   for(var p = 0; subQueries && p< subQueries.length; p++) {
    let currentAlias = subQueries[p].alias+".";
    alias[currentAlias] = {};
    alias[currentAlias]["columns"] = [];
    alias[currentAlias]["combinations"] = [];

    let columns = subQueries[p].columns;
    for(var q=0; q<columns.length; q++) {
     if(columns[q].identifierChain && columns[q].identifierChain.length === 2){
       alias[currentAlias]["columns"].push(columns[q].identifierChain[1].name);
       alias[currentAlias]["combinations"].push(currentAlias+columns[q].identifierChain[1].name);
     } else {
       alias[currentAlias]["columns"]= this.formTableColumns(dbTables, cm, "", columns[q].tables[0].identifierChain[0].name);
       alias[currentAlias]["combinations"] = this.formTableColumns(dbTables, cm, subQueries[p].alias?subQueries[p].alias+".":"", columns[q].tables[0].identifierChain[0].name);
     }
    }
   }
   return alias;
  },
  findAliasFromMeta(suggestions) {
    let aliasRefTables = [];
    for(let suggestion of suggestions.locations) {
        if(suggestion.alias) {
            aliasRefTables.push(suggestion);
        }
    }
    return aliasRefTables;
  },
  parseAlias(suggestions, dbTables, cm) {
   let aliasRef = suggestions.suggestAggregateFunctions, alias = {}, aliasRefTables = [], aliasList = {};
   if(aliasRef){
        aliasRefTables = aliasRef.tables
   } else {
        aliasRefTables = this.findAliasFromMeta(suggestions);
   }
   for(var p = 0; aliasRefTables && p< aliasRefTables.length; p++) {
    if(!aliasRefTables[p].alias || !aliasRefTables[p].identifierChain) {
        continue;
    }
    let currentAlias = aliasRefTables[p].alias+".";
    alias[currentAlias] = {};
    alias[currentAlias]["columns"] = [];
    alias[currentAlias]["combinations"] = [];

    let columns = aliasRefTables[p].identifierChain[0], tabName = columns.name;

    alias[currentAlias]["columns"]= this.formTableColumns(dbTables, cm, "", tabName);
    alias[currentAlias]["combinations"] = this.formTableColumns(dbTables, cm, aliasRefTables[p].alias?aliasRefTables[p].alias+".":"", tabName);
    aliasList[aliasRefTables[p].identifierChain[0].name] = aliasRefTables[p].alias;
   }
   return {alias, aliasList};
  },
  formTableColumns(dbTables, cm, tokenToAppend, tableToFilter) {
    var tableColList = [];
    for(var p=0; p<dbTables.length; p++){
        var tables = dbTables[p].tables;
        for(var i=0; i<tables.length; i++){
          for(var j=0; j<tables[i].columns.length; j++) {
            if(tableToFilter === tables[i].name || tableToFilter === tables[i].name+";") {
              if(tokenToAppend){
                tableColList.push(tokenToAppend+tables[i].columns[j]);
              } else {
                tableColList.push(tables[i].columns[j]);
              }
            }
          }
        }
    }
    return tableColList;
  },
  formTables(dbTables, cm, tokenToAppend) {
    var tableColList = [], lastWord;
    for(var p=0; p<dbTables.length; p++) {
        var tables = dbTables[p].tables;
        for(var i=0; i<tables.length; i++) {
          if(tokenToAppend) {
              tableColList.push(tokenToAppend + tables[i].name);
          } else {
              tableColList.push(tables[i].name);
          }
        }
    }
    return tableColList;
  },
  formDatabases(tableColList, databases=[], tokenToAppend="") {
    tableColList = this.insertHeadersToSuggestions(tableColList, "Database");
    databases.forEach(function(item){
      let token = {"text":item.get("name")+tokenToAppend, "className":"hint-custom"};
      tableColList.push(token);
    });
    return tableColList;
  },
  actions:{
  }

});
