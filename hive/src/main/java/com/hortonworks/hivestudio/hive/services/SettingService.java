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

import com.hortonworks.hivestudio.common.exception.generic.ConstraintViolationException;
import com.hortonworks.hivestudio.common.exception.generic.ItemNotFoundException;
import com.hortonworks.hivestudio.common.exception.generic.UnauthorizedException;
import com.hortonworks.hivestudio.common.repository.transaction.DASTransaction;
import com.hortonworks.hivestudio.hive.persistence.entities.Setting;
import com.hortonworks.hivestudio.hive.persistence.repositories.SettingRepository;

import lombok.extern.slf4j.Slf4j;

/**
 * Service to interact with Hive ide Settings
 */
@Slf4j
public class SettingService {
  private final Provider<SettingRepository> settingRepositoryProvider;

  @Inject
  public SettingService(Provider<SettingRepository> settingRepositoryProvider) {
    this.settingRepositoryProvider = settingRepositoryProvider;
  }

  public Collection<Setting> getAllForUser(String username) {
    return settingRepositoryProvider.get().findAllByOwner(username);
  }

  @DASTransaction
  public Setting createSetting(Setting setting, String username) {
    SettingRepository settingRepository = settingRepositoryProvider.get();
    settingRepository.findByKeyAndOwner(setting.getKey(), username).ifPresent((x) -> {
      log.error("Cannot create new settings. Settings with key `{}` already exists", setting.getKey());
      throw new ConstraintViolationException("Cannot create new settings. Settings with key '" + setting.getKey() + "' already exists");
    });
    setting.setOwner(username);

    settingRepository.save(setting);
    return setting;
  }

  @DASTransaction
  public Setting updateSetting(Integer id, Setting setting, String username) {
    Optional<Setting> one = settingRepositoryProvider.get().findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("Setting not found for id: " + id));

    Setting entity = one.get();
    if (!entity.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot update setting with id {}. Authorization failed for setting", id);
      throw new UnauthorizedException("Not owner of setting with id " + id);
    }

    if (!entity.getKey().equalsIgnoreCase(setting.getKey())) {
      settingRepositoryProvider.get().findByKeyAndOwner(setting.getKey(), username).ifPresent((x) -> {
        log.error("Cannot update settings with id {}. Settings with key `{}` already exists", id, setting.getKey());
        throw new ConstraintViolationException("Cannot update settings with id " + id + ". Settings with key '" + setting.getKey() + "' already exists");
      });
    }

    entity.setKey(setting.getKey());
    entity.setValue(setting.getValue());
    return entity;
  }

  @DASTransaction
  public void removeSetting(Integer id, String username) {
    Optional<Setting> one = settingRepositoryProvider.get().findOne(id);
    one.orElseThrow(() -> new ItemNotFoundException("Setting not found for id: " + id));
    Setting entity = one.get();
    if (!entity.getOwner().equalsIgnoreCase(username)) {
      log.error("Cannot delete setting with id {}. Authorization failed for setting", id);
      throw new UnauthorizedException("Not owner of setting with id " + id);
    }
    settingRepositoryProvider.get().delete(entity.getId());
  }


  /**
   * Wrapper class for settings request
   */
  public static class SettingRequest {
    private Setting setting;

    public Setting getSetting() {
      return setting;
    }

    public void setSetting(Setting setting) {
      this.setting = setting;
    }
  }
}
