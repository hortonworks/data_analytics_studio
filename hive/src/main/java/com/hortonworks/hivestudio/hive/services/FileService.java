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
import com.hortonworks.hivestudio.hive.persistence.repositories.FileRepository;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Service to interact with the File Resources
 */
@Slf4j
public class FileService {
  private final Provider<FileRepository> fileRepositoryProvider;

  @Inject
  public FileService(Provider<FileRepository> fileRepositoryProvider) {
    this.fileRepositoryProvider = fileRepositoryProvider;
  }

  public File getOne(Integer id, String username) {
    Optional<File> fileOptional = fileRepositoryProvider.get().findOne(id);
    fileOptional.orElseThrow(() -> new ItemNotFoundException("File resource not found for id: " + id));

    File file = fileOptional.get();
    if (!file.getOwner().equalsIgnoreCase(username)) {
      throw new UnauthorizedException("Not owner of File resource with id " + id);
    }

    return file;
  }

  public Collection<File> getAllForUser(String username) {
    return fileRepositoryProvider.get().findAllByOwner(username);
  }

  @DASTransaction
  public File createFileResource(File file, String username) {
    FileRepository fileRepository = fileRepositoryProvider.get();
    file.setOwner(username);
    fileRepository.save(file);
    return file;
  }

  @DASTransaction
  public File updateFileResouce(Integer id, File file, String username) {
    Optional<File> one = fileRepositoryProvider.get().findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("File resource not found for id: " + id));


    File entity = one.get();
    if (!entity.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot update File resource with id {}. Authorization failed for setting", id);
      throw new UnauthorizedException("Not owner of File resource with id " + id);
    }

    entity.setName(file.getName());
    entity.setPath(file.getPath());
    return entity;
  }

  public void removeUdf(Integer id, String username) {
    FileRepository fileRepository = fileRepositoryProvider.get();
    Optional<File> one = fileRepository.findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("File resource not found for id: " + id));
    File entity = one.get();
    if (!entity.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot delete File resource with id {}. Authorization failed for setting", id);
      throw new UnauthorizedException("Not owner of File resource with id " + id);
    }
    fileRepository.delete(entity.getId());
  }

  /**
   * Wrapper class for UDF request
   */
  @Data
  public static class FileRequest {
    private File fileResource;
  }
}
