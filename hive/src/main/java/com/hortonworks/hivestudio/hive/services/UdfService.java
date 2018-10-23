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
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.hive.persistence.entities.File;
import com.hortonworks.hivestudio.hive.persistence.entities.Udf;
import com.hortonworks.hivestudio.hive.persistence.repositories.FileRepository;
import com.hortonworks.hivestudio.hive.persistence.repositories.UdfRepository;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Service to interact with the UDFs
 */
@Slf4j
public class UdfService {
  private final Provider<UdfRepository> udfRepositoryProvider;
  private final Provider<FileRepository> fileRepositoryProvider;

  @Inject
  public UdfService(Provider<UdfRepository> udfRepositoryProvider, Provider<FileRepository> fileRepositoryProvider) {
    this.udfRepositoryProvider = udfRepositoryProvider;
    this.fileRepositoryProvider = fileRepositoryProvider;
  }

  public Udf getOne(Integer id, String username) {
    Optional<Udf> udfOptional = udfRepositoryProvider.get().findOne(id);
    udfOptional.orElseThrow(() -> new ItemNotFoundException("UDF not found for id: " + id));

    Udf udf = udfOptional.get();
    if (!udf.getOwner().equalsIgnoreCase(username)) {
      throw new UnauthorizedException("Not owner of udf with id " + id);
    }

    return udf;
  }

  public Collection<Udf> getAllForUser(String username) {
    return udfRepositoryProvider.get().findAllByOwner(username);
  }

  @DASTransaction
  public Udf createUdf(Udf udf, String username) {
    UdfRepository udfRepository = udfRepositoryProvider.get();
    FileRepository fileRepository = fileRepositoryProvider.get();

    Optional<File> fileOptional = fileRepository.findOne(udf.getFileResource());
    fileOptional.orElseThrow(() -> new ItemNotFoundException("File Resource not found for id: " + udf.getFileResource()));

    udf.setOwner(username);
    udfRepository.save(udf);
    return udf;
  }

  @DASTransaction
  public Udf updateUdf(Integer id, Udf udf, String username) {
    Optional<Udf> one = udfRepositoryProvider.get().findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("Udf not found for id: " + id));

    FileRepository fileRepository = fileRepositoryProvider.get();
    Optional<File> fileOptional = fileRepository.findOne(udf.getFileResource());
    fileOptional.orElseThrow(() -> new ItemNotFoundException("File Resource not found for id: " + udf.getFileResource()));

    Udf entity = one.get();
    if (!entity.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot update Udf with id {}. Authorization failed for setting", id);
      throw new UnauthorizedException("Not owner of Udf with id " + id);
    }

    entity.setName(udf.getName());
    entity.setClassname(udf.getClassname());
    entity.setFileResource(udf.getFileResource());
    return entity;
  }

  @DASTransaction
  public void removeUdf(Integer id, String username) {
    Optional<Udf> one = udfRepositoryProvider.get().findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("Udf not found for id: " + id));
    Udf entity = one.get();
    if (!entity.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot delete Udf with id {}. Authorization failed for setting", id);
      throw new UnauthorizedException("Not owner of Udf with id " + id);
    }
    udfRepositoryProvider.get().delete(entity.getId());
  }


  /**
   * Wrapper class for UDF request
   */
  @Data
  public static class UdfRequest {
    private Udf udf;
  }
}
