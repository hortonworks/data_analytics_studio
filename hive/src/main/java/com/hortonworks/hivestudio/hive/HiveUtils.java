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
package com.hortonworks.hivestudio.hive;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.hortonworks.hivestudio.common.hdfs.HdfsContext;
import com.hortonworks.hivestudio.hive.persistence.entities.Setting;
import com.hortonworks.hivestudio.hive.services.SettingService;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.Collection;

@Slf4j
public class HiveUtils {
  private SettingService settingsService;

  @Inject
  public HiveUtils(SettingService settingsService){
    this.settingsService = settingsService;
  }
  public HdfsContext createHdfsContext(HiveContext hiveContext) {
    return new HdfsContext(hiveContext.getUsername());
  }

  public Optional<String> getSettingsString(String username) {
    Collection<Setting> settings = settingsService.getAllForUser(username);
    if (null != settings && !settings.isEmpty()) {
      return Optional.of(Joiner.on(";\n").join(FluentIterable.from(settings).transform(new Function<Setting, String>() {
        @Override
        public String apply(Setting setting) {
          return "set " + setting.getKey() + "=" + setting.getValue();
        }
      }).toList()) + ";\n"/*need this ;\n at the end of last line also.*/);
    } else {
      return Optional.absent();
    }
  }


}
