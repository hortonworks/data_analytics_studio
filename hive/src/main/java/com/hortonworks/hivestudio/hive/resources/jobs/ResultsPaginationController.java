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


package com.hortonworks.hivestudio.hive.resources.jobs;


import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.common.exception.ServiceFormattedException;
import com.hortonworks.hivestudio.hive.HiveContext;
import com.hortonworks.hivestudio.hive.client.AsyncJobRunner;
import com.hortonworks.hivestudio.hive.client.AsyncJobRunnerImpl;
import com.hortonworks.hivestudio.hive.client.Row;
import com.hortonworks.hivestudio.hive.ConnectionSystem;
import com.hortonworks.hivestudio.hive.client.ColumnDescription;
import com.hortonworks.hivestudio.hive.client.Cursor;
import com.hortonworks.hivestudio.hive.client.EmptyCursor;
import com.hortonworks.hivestudio.hive.client.HiveClientException;
import com.hortonworks.hivestudio.hive.client.NonPersistentCursor;
import com.hortonworks.hivestudio.hive.utils.BadRequestFormattedException;
import com.hortonworks.hivestudio.hive.utils.ResultFetchFormattedException;
import com.hortonworks.hivestudio.hive.utils.ResultNotReadyFormattedException;
import org.apache.commons.collections4.map.PassiveExpiringMap;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Results Pagination Controller
 * Persists cursors for result sets
 */
public class ResultsPaginationController {
  private ConnectionSystem system;
  public static final String DEFAULT_SEARCH_ID = "default";
//  private static Map<String, ResultsPaginationController> viewSingletonObjects = new HashMap<String, ResultsPaginationController>();
//  private static ResultsPaginationController theOnlyOne = new ResultsPaginationController();
//  public static ResultsPaginationController getInstance(HiveContext context) {
//    // removed per instance values
////    if (!viewSingletonObjects.containsKey(context.getInstanceName()))
////      viewSingletonObjects.put(context.getInstanceName(), new ResultsPaginationController());
////    return viewSingletonObjects.get(context.getInstanceName());
//    // TODO : remove the only one with Singleton Provider pattern
//    return theOnlyOne;
//  }

  @Inject
  public ResultsPaginationController(ConnectionSystem connectionSystem) {
    this.system = connectionSystem;
  }

  private static final long EXPIRING_TIME = 10*60*1000;  // 10 minutes
  private static final int DEFAULT_FETCH_COUNT = 50;
  private Map<String, Cursor<Row, ColumnDescription>> resultsCache;

  public Response getResultAsResponse(final Integer jobId, final String fromBeginning, Integer count, String searchId, String format, String requestedColumns, HiveContext context, Configuration configuration) throws HiveClientException {
    final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(configuration, system.getOperationController(context), system.getActorSystem());

    return this
            .request(Integer.toString(jobId), searchId, true, fromBeginning, count, format,requestedColumns,
              createCallableMakeResultSets(jobId, fromBeginning, context, asyncJobRunner)).build();
  }

  public ResultsResponse getResult(final Integer jobId, final String fromBeginning, Integer count, String
    searchId, String requestedColumns, HiveContext context, Configuration configuration)  {
    final AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl(configuration, system.getOperationController(context), system.getActorSystem());

    return this
            .fetchResult(Integer.toString(jobId), searchId, true, fromBeginning, count, requestedColumns,
              createCallableMakeResultSets(jobId, fromBeginning, context, asyncJobRunner));
  }

  private Callable<Cursor<Row, ColumnDescription>> createCallableMakeResultSets(final Integer jobId, final String
    fromBeginning, final HiveContext context, final AsyncJobRunner asyncJobRunner) {
    return new Callable<Cursor< Row, ColumnDescription >>() {
      @Override
      public Cursor call() throws Exception {
        Optional<NonPersistentCursor> cursor;
        if(fromBeginning != null && fromBeginning.equals("true")){
          cursor = asyncJobRunner.resetAndGetCursor(jobId, context);
        }
        else {
          cursor = asyncJobRunner.getCursor(jobId, context);
        }
        if(cursor.isPresent())
        return cursor.get();
        else
          return new EmptyCursor();
      }
    };
  }

  public class CustomTimeToLiveExpirationPolicy extends PassiveExpiringMap.ConstantTimeToLiveExpirationPolicy<String, Cursor<Row, ColumnDescription>> {
    public CustomTimeToLiveExpirationPolicy(long timeToLiveMillis) {
      super(timeToLiveMillis);
    }

    @Override
    public long expirationTime(String key, Cursor<Row, ColumnDescription> value) {
      if (key.startsWith("$")) {
        return -1;  //never expire
      }
      return super.expirationTime(key, value);
    }
  }

  private Map<String, Cursor<Row, ColumnDescription>> getResultsCache() {
    if (resultsCache == null) {
      PassiveExpiringMap<String, Cursor<Row, ColumnDescription>> resultsCacheExpiringMap =
          new PassiveExpiringMap<>(new CustomTimeToLiveExpirationPolicy(EXPIRING_TIME));
      resultsCache = Collections.synchronizedMap(resultsCacheExpiringMap);
    }
    return resultsCache;
  }

  /**
   * Renew timer of cache entry.
   * @param key name/id of results request
   * @return false if entry not found; true if renew was ok
   */
  public boolean keepAlive(String key, String searchId) {
    if (searchId == null)
      searchId = DEFAULT_SEARCH_ID;
    String effectiveKey = key + "?" + searchId;
    if (!getResultsCache().containsKey(effectiveKey)) {
      return false;
    }
    Cursor cursor = getResultsCache().get(effectiveKey);
    getResultsCache().put(effectiveKey, cursor);
    cursor.keepAlive();
    return true;
  }

  private Cursor<Row, ColumnDescription> getResultsSet(String key, Callable<Cursor<Row, ColumnDescription>> makeResultsSet) {
    if (!getResultsCache().containsKey(key)) {
      Cursor resultSet;
      try {
        resultSet = makeResultsSet.call();
        if (resultSet.isResettable()) {
          resultSet.reset();
        }
      } catch (ResultNotReadyFormattedException | ResultFetchFormattedException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new ServiceFormattedException(ex.getMessage(), ex);
      }
      getResultsCache().put(key, resultSet);
    }

    return getResultsCache().get(key);
  }

  /**
   * returns the results in standard format
   * @param key
   * @param searchId
   * @param canExpire
   * @param fromBeginning
   * @param count
   * @param requestedColumns
   * @param makeResultsSet
   * @return
   * @throws HiveClientException
   */
  public ResultsResponse fetchResult(String key, String searchId, boolean canExpire, String fromBeginning, Integer
    count, String requestedColumns, Callable<Cursor<Row, ColumnDescription>> makeResultsSet) {

    ResultProcessor resultProcessor = new ResultProcessor(key, searchId, canExpire, fromBeginning, count, requestedColumns, makeResultsSet).invoke();
    List<Object[]> rows = resultProcessor.getRows();
    List<ColumnDescription> schema = resultProcessor.getSchema();
    Cursor<Row, ColumnDescription> resultSet = resultProcessor.getResultSet();

    int read = rows.size();
    return getResultsResponse(rows, schema, resultSet, read);
  }

  /**
   * returns the results in either D3 format or starndard format wrapped inside ResponseBuilder object.
   * @param key
   * @param searchId
   * @param canExpire
   * @param fromBeginning
   * @param count : number of rows to fetch
   * @param format : 'd3' or empty
   * @param requestedColumns
   * @param makeResultsSet
   * @return
   * @throws HiveClientException
   */
  public Response.ResponseBuilder request(String key, String searchId, boolean canExpire, String fromBeginning, Integer count, String format, String requestedColumns, Callable<Cursor<Row, ColumnDescription>> makeResultsSet) throws HiveClientException {
    ResultProcessor resultProcessor = new ResultProcessor(key, searchId, canExpire, fromBeginning, count, requestedColumns, makeResultsSet).invoke();
    List<Object[]> rows = resultProcessor.getRows();
    List<ColumnDescription> schema = resultProcessor.getSchema();
    Cursor<Row, ColumnDescription> resultSet = resultProcessor.getResultSet();

    int read = rows.size();
    if(format != null && format.equalsIgnoreCase("d3")) {
      List<Map<String, Object>> results = getD3FormattedResult(rows, schema);
      return Response.ok(results);
    } else {
      ResultsResponse resultsResponse = getResultsResponse(rows, schema, resultSet, read);
      return Response.ok(resultsResponse);
    }
  }

  public List<Map<String, Object>> getD3FormattedResult(List<Object[]> rows, List<ColumnDescription> schema) {
    List<Map<String,Object>> results = new ArrayList<>();
    for(int i=0; i<rows.size(); i++) {
      Object[] row = rows.get(i);
      Map<String, Object> keyValue = new HashMap<>(row.length);
      for(int j=0; j<row.length; j++) {
        //Replace dots in schema with underscore
        String schemaName = schema.get(j).getName();
        keyValue.put(schemaName.replace('.','_'), row[j]);
      }
      results.add(keyValue);
    } return results;
  }

  public ResultsResponse getResultsResponse(List<Object[]> rows, List<ColumnDescription> schema, Cursor<Row, ColumnDescription> resultSet, int read) {
    ResultsResponse resultsResponse = new ResultsResponse();
    resultsResponse.setSchema(schema);
    resultsResponse.setRows(rows);
    resultsResponse.setReadCount(read);
    resultsResponse.setHasNext(resultSet.hasNext());
    //      resultsResponse.setSize(resultSet.size());
    resultsResponse.setOffset(resultSet.getOffset());
    resultsResponse.setHasResults(true);
    return resultsResponse;
  }

  private <T> List<T> filter(List<T> list, Set<Integer> selectedColumns) {
    List<T> filtered = Lists.newArrayList();
    for(int i: selectedColumns) {
      if(list != null && list.get(i) != null)
        filtered.add(list.get(i));
    }

    return filtered;
  }

  private Set<Integer> getRequestedColumns(String requestedColumns) {
    if(Strings.isNullOrEmpty(requestedColumns)) {
      return new HashSet<>();
    }
    Set<Integer> selectedColumns = Sets.newHashSet();
    for (String columnRequested : requestedColumns.split(",")) {
      try {
        selectedColumns.add(Integer.parseInt(columnRequested));
      } catch (NumberFormatException ex) {
        throw new BadRequestFormattedException("Columns param should be comma-separated integers", ex);
      }
    }
    return selectedColumns;
  }

  private class ResultProcessor {
    private String key;
    private String searchId;
    private boolean canExpire;
    private String fromBeginning;
    private Integer count;
    private String requestedColumns;
    private Callable<Cursor<Row, ColumnDescription>> makeResultsSet;
    private Cursor<Row, ColumnDescription> resultSet;
    private List<ColumnDescription> schema;
    private List<Object[]> rows;

    public ResultProcessor(String key, String searchId, boolean canExpire, String fromBeginning, Integer count, String requestedColumns, Callable<Cursor<Row, ColumnDescription>> makeResultsSet) {
      this.key = key;
      this.searchId = searchId;
      this.canExpire = canExpire;
      this.fromBeginning = fromBeginning;
      this.count = count;
      this.requestedColumns = requestedColumns;
      this.makeResultsSet = makeResultsSet;
    }

    public Cursor<Row, ColumnDescription> getResultSet() {
      return resultSet;
    }

    public List<ColumnDescription> getSchema() {
      return schema;
    }

    public List<Object[]> getRows() {
      return rows;
    }

    public ResultProcessor invoke() {
      if (searchId == null)
        searchId = DEFAULT_SEARCH_ID;
      key = key + "?" + searchId;
      if (!canExpire)
        key = "$" + key;
      if (fromBeginning != null && fromBeginning.equals("true") && getResultsCache().containsKey(key)) {
        getResultsCache().remove(key);
      }

      resultSet = getResultsSet(key, makeResultsSet);

      if (count == null)
        count = DEFAULT_FETCH_COUNT;

      List<ColumnDescription> allschema = resultSet.getDescriptions();
      List<Row> allRowEntries = FluentIterable.from(resultSet)
        .limit(count).toList();

      schema = allschema;

      final Set<Integer> selectedColumns = getRequestedColumns(requestedColumns);
      if (!selectedColumns.isEmpty()) {
        schema = filter(allschema, selectedColumns);
      }

      rows = FluentIterable.from(allRowEntries)
        .transform(new Function<Row, Object[]>() {
          @Override
          public Object[] apply(Row input) {
            if (!selectedColumns.isEmpty()) {
              return filter(Lists.newArrayList(input.getRow()), selectedColumns).toArray();
            } else {
              return input.getRow();
            }
          }
        }).toList();
      return this;
    }
  }
}
