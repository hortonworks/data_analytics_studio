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
package com.hortonworks.hivestudio.eventProcessor.services;

import java.time.LocalDateTime;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerAuditType;
import com.hortonworks.hivestudio.eventProcessor.entities.SchedulerRunAudit;
import com.hortonworks.hivestudio.eventProcessor.entities.repository.SchedulerRunAuditRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@DASTransaction
public class AuditDataService {

  private static final Integer MAX_RETRY_COUNT = 3;
  private final Provider<SchedulerRunAuditRepository> auditRepositoryProvider;

  @Inject
  public AuditDataService(Provider<SchedulerRunAuditRepository> auditRepositoryProvider) {
    this.auditRepositoryProvider = auditRepositoryProvider;
  }

  public Optional<SchedulerRunAudit> getLastAuditEntry(SchedulerAuditType type) {
    SchedulerRunAuditRepository repository = auditRepositoryProvider.get();
    return repository.findLastAuditEntryByType(type);
  }

  public Optional<SchedulerRunAudit> getNextReadInformation(SchedulerAuditType type) {
    SchedulerRunAuditRepository repository = auditRepositoryProvider.get();
    Optional<SchedulerRunAudit> lastAuditEntry = repository.findLastAuditEntryByType(type);
    if (shouldCreateNewEntry(lastAuditEntry)) {
      return Optional.of(createAndGetNewAuditEntry(lastAuditEntry, repository, type));
    } else {
      return Optional.empty();
    }
  }

  public Optional<SchedulerRunAudit> getNextReadInformation(SchedulerRunAudit lastAuditEntry, SchedulerAuditType type) {
    SchedulerRunAuditRepository repository = auditRepositoryProvider.get();
    if (shouldCreateNewEntry(Optional.ofNullable(lastAuditEntry))) {
      return Optional.of(createAndGetNewAuditEntry(Optional.ofNullable(lastAuditEntry), repository, type));
    } else {
      return Optional.empty();
    }
  }

  public void updateWithError(Integer id, Throwable reason) {
    log.error("Report processor with Audit id {} failed. Updating the audit to 'ERROR'", id);
    SchedulerRunAuditRepository repository = auditRepositoryProvider.get();
    Optional<SchedulerRunAudit> auditOptional = repository.findOne(id);
    if(!auditOptional.isPresent()) {
      log.error("Could not update scheduler audit entry. Record not found for id {}", id);
      return;
    }

    SchedulerRunAudit audit = auditOptional.get();
    audit.setReadEndTime(LocalDateTime.now());
    audit.setStatus("ERROR");
    audit.setFailureReason(ExceptionUtils.getMessage(reason) + "\n" + ExceptionUtils.getStackTrace(reason));
    repository.save(audit);
  }

  public void updateSuccess(Integer id) {
    log.info("Updating the Audit with id {} to 'SUCCESS'", id);
    SchedulerRunAuditRepository repository = auditRepositoryProvider.get();
    Optional<SchedulerRunAudit> auditOptional = repository.findOne(id);
    if(!auditOptional.isPresent()) {
      log.error("Could not update scheduler audit entry. Record not found for id {}", id);
      return;
    }

    SchedulerRunAudit audit = auditOptional.get();
    audit.setReadEndTime(LocalDateTime.now());
    audit.setStatus("SUCCESS");
    repository.save(audit);
  }

  public void updateHiveIdsProcessed(Integer id, String ids) {
    SchedulerRunAuditRepository repository = auditRepositoryProvider.get();
    Optional<SchedulerRunAudit> auditOptional = repository.findOne(id);
    if(!auditOptional.isPresent()) {
      log.error("Could not update scheduler audit entry. Record not found for id {}", id);
      return;
    }

    SchedulerRunAudit audit = auditOptional.get();
    audit.setQueriesProcessed(ids);
    repository.save(audit);
  }

  private boolean shouldCreateNewEntry(Optional<SchedulerRunAudit> lastAuditEntry) {
    if (lastAuditEntry.isPresent()) {
      SchedulerRunAudit newEntry = lastAuditEntry.get();
      return !newEntry.getStatus().equalsIgnoreCase("READING");
    } else {
      return true;
    }
  }

  private SchedulerRunAudit createAndGetNewAuditEntry(Optional<SchedulerRunAudit> lastAuditEntryOptional, SchedulerRunAuditRepository repository, SchedulerAuditType type) {
    SchedulerRunAudit newEntry = new SchedulerRunAudit();
    newEntry.setReadStartTime(LocalDateTime.now());
    newEntry.setType(type);
    newEntry.setStatus("READING");
    if(lastAuditEntryOptional.isPresent()) {
      SchedulerRunAudit lastAuditEntry = lastAuditEntryOptional.get();
      if(lastAuditEntry.getStatus().equalsIgnoreCase("ERROR") && (lastAuditEntry.getRetryCount() == null || lastAuditEntry.getRetryCount() < MAX_RETRY_COUNT - 1)) {
        newEntry.setLastTryId(lastAuditEntry.getId());
        Integer retryCount = lastAuditEntry.getRetryCount() == null ? 1 : lastAuditEntry.getRetryCount() + 1;
        newEntry.setRetryCount(retryCount);
        newEntry.setQueriesProcessed(lastAuditEntry.getQueriesProcessed());
      }
    }

    return repository.save(newEntry);
  }
}
