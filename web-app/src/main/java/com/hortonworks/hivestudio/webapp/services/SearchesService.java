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
package com.hortonworks.hivestudio.webapp.services;

import java.util.Collection;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.hive.persistence.entities.SuggestedSearch;
import com.hortonworks.hivestudio.hive.persistence.repositories.SuggestedSearchRepository;

import lombok.Data;

@Singleton
public class SearchesService {

  private final Provider<SuggestedSearchRepository> searchesRepositoryProvider;

  @Inject
  public SearchesService(Provider<SuggestedSearchRepository> searchesRepositoryProvider) {
    this.searchesRepositoryProvider = searchesRepositoryProvider;
  }

  public Collection<SuggestedSearch> getAllSuggested(String entity) {
    return searchesRepositoryProvider.get().findAllByEntityCategoryOwner(entity, SuggestedSearch.CategoryTypes.SUGGEST.name(), "DEFAULT");
  }

  public Collection<SuggestedSearch> getAllSaved(String entity, String owner) {
    return searchesRepositoryProvider.get().findAllByEntityCategoryOwner(entity, SuggestedSearch.CategoryTypes.SAVED.name(), owner);
  }

  @DASTransaction
  public SuggestedSearch createSaved(SuggestedSearch suggestedSearch, String owner) {
    suggestedSearch.setOwner(owner);
    suggestedSearch.setCategory(SuggestedSearch.CategoryTypes.SAVED);
    searchesRepositoryProvider.get().save(suggestedSearch);
    return suggestedSearch;
  }

  @DASTransaction
  public SuggestedSearch delete(Integer id) {
    Optional<SuggestedSearch> search = searchesRepositoryProvider.get().findOne(id);
    searchesRepositoryProvider.get().delete(search.orElseThrow(() -> new ItemNotFoundException("Search item with id : " + id)).getId());
    return search.get();
  }

  /**
   * Wrapper object for json mapping
   */
  @Data
  public static class SearchesRequest {
    private SuggestedSearch suggestedSearch;
  }

}
