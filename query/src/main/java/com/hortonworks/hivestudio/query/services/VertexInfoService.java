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
package com.hortonworks.hivestudio.query.services;

import java.util.Collection;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;

import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.entities.VertexInfo;
import com.hortonworks.hivestudio.query.entities.repositories.VertexInfoRepository;

import lombok.extern.slf4j.Slf4j;

/**
 * Service to fetch the Dag Information
 */
@Slf4j
public class VertexInfoService {
  private final Provider<VertexInfoRepository> repositoryProvider;

  @Inject
  public VertexInfoService(Provider<VertexInfoRepository> repositoryProvider) {
    this.repositoryProvider = repositoryProvider;
  }

  public VertexInfo getOne(Long id) {
    VertexInfoRepository repository = repositoryProvider.get();
    Optional<VertexInfo> vertexInfo = repository.findOne(id);
    vertexInfo.orElseThrow(() -> new ItemNotFoundException("Dag Information with id '" + id + "' not found"));
    return vertexInfo.get();
  }

  public VertexInfo getOneByVertexId(String vertexId) {
    VertexInfoRepository repository = repositoryProvider.get();
    Optional<VertexInfo> vertexInfoOptional = repository.findByVertexId(vertexId);
    vertexInfoOptional.orElseThrow(() -> new ItemNotFoundException("Dag Information with dagId '" + vertexId + "' not found"));
    return  vertexInfoOptional.get();
  }

  public Collection<VertexInfo> getVerticesByDagId(String dagId) {
    VertexInfoRepository repository = repositoryProvider.get();
    return repository.findAllByDagId(dagId);
  }
}
