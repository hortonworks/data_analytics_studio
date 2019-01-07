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
package com.hortonworks.hivestudio.hive.services;

import java.util.Collection;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.exception.generic.UnauthorizedException;
import com.hortonworks.hivestudio.hive.persistence.entities.SavedQuery;
import com.hortonworks.hivestudio.hive.persistence.repositories.SavedQueryRepository;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Service to interact with the queries that are saved by the user.
 */
@Slf4j
public class SavedQueryService {

  private final Provider<SavedQueryRepository> queryRepositoryProvider;

  @Inject
  public SavedQueryService(Provider<SavedQueryRepository> queryRepositoryProvider) {
    this.queryRepositoryProvider = queryRepositoryProvider;
  }

  public SavedQuery getOne(Integer id, String username) {
    Optional<SavedQuery> one = queryRepositoryProvider.get().findOne(id);
    SavedQuery query = one.orElseThrow(() -> new ItemNotFoundException("Saved Query not found for id: " + id));
    if (!query.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot update Saved query with id {}. Authorization failed.", id);
      throw new UnauthorizedException("Not owner of Saved query with id " + id);
    }
    query.setShortQuery(makeShortQuery(query.getQuery()));
    return query;
  }

  public Collection<SavedQuery> getAllForUser(String username) {
    Collection<SavedQuery> queries = queryRepositoryProvider.get().findAllByOwner(username);
    for (SavedQuery query : queries) {
      query.setShortQuery(makeShortQuery(query.getQuery()));
    }
    return queries;
  }

  public void removeSavedQuery(Integer id, String username) {
    Optional<SavedQuery> one = queryRepositoryProvider.get().findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("Saved Query not found for id: " + id));
    SavedQuery query = one.get();
    if (!query.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot update Saved query with id {}. Authorization failed.", id);
      throw new UnauthorizedException("Not owner of Saved query with id " + id);
    }
    queryRepositoryProvider.get().delete(query.getId());
  }

  public SavedQuery createSavedQuery(SavedQuery savedQuery, String username) {
    savedQuery.setOwner(username);
    return queryRepositoryProvider.get().save(savedQuery);
  }

  public SavedQuery updateSavedQuery(Integer id, SavedQuery savedQuery, String username) {
    Optional<SavedQuery> one = queryRepositoryProvider.get().findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("Saved Query not found for id: " + id));
    SavedQuery query = one.get();
    if (!query.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot update Saved query with id {}. Authorization failed.", id);
      throw new UnauthorizedException("Not owner of Saved query with id " + id);
    }
    query.setSelectedDatabase(savedQuery.getSelectedDatabase());
    query.setQuery(savedQuery.getQuery());
    query.setTitle(savedQuery.getTitle());
    return queryRepositoryProvider.get().save(query);
  }

  /**
   * Generate short preview of query.
   * Remove SET settings like "set hive.execution.engine=tez;" from beginning
   * and trim to 42 symbols.
   *
   * @param query full query
   * @return shortened query
   */
  protected static String makeShortQuery(String query) {
    query = query.replaceAll("(?i)set\\s+[\\w\\-.]+(\\s*)=(\\s*)[\\w\\-.]+(\\s*);", "");
    query = query.trim();
    return query.substring(0, (query.length() > 42) ? 42 : query.length());
  }

  /**
   * Wrapper object for json mapping
   */
  @Data
  public static class SavedQueryRequest {
    private SavedQuery savedQuery;
  }
}
