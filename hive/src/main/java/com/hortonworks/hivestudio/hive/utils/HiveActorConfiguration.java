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
package com.hortonworks.hivestudio.hive.utils;

import java.util.Optional;

import com.hortonworks.hivestudio.common.config.Configuration;

/**
 * This fetches the configuration for the actor system from ambari.properties
 */
public class HiveActorConfiguration {
  private static final String DEFAULT_CONFIG = "default";
  private static final String CONNECTION_PREFIX = "views.ambari.hive.";
  private static final String CONNECTION_INACTIVITY_TIMEOUT_PATTERN = CONNECTION_PREFIX + "%s.connection.inactivity.timeout";
  private static final String CONNECTION_TERMINATION_TIMEOUT_PATTERN = CONNECTION_PREFIX + "%s.connection.termination.timeout";
  private static final String SYNC_QUERY_TIMEOUT_PATTERN = CONNECTION_PREFIX + "%s.sync.query.timeout";
  private static final String REPL_QUERY_TIMEOUT_PATTERN = CONNECTION_PREFIX + "%s.repl.query.timeout";
  private static final String RESULT_FETCH_TIMEOUT_PATTERN = CONNECTION_PREFIX + "%s.result.fetch.timeout";
  private static final long DEFAULT_REPL_TIMEOUT = 900 * 1000;
  private static final long DEFAULT_SYNC_TIMEOUT = 300 * 1000;

  private final Configuration configuration;

  public HiveActorConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  public long getInactivityTimeout(long defaultValue) {
    return getPropertiesFromConfiguration(CONNECTION_INACTIVITY_TIMEOUT_PATTERN, defaultValue);
  }

  public long getTerminationTimeout(long defaultValue) {
    return getPropertiesFromConfiguration(CONNECTION_TERMINATION_TIMEOUT_PATTERN, defaultValue);
  }

  public long getSyncQueryTimeout() {
    return getPropertiesFromConfiguration(SYNC_QUERY_TIMEOUT_PATTERN, DEFAULT_SYNC_TIMEOUT);
  }

  public long getResultFetchTimeout(long defaultValue) {
    return getPropertiesFromConfiguration(RESULT_FETCH_TIMEOUT_PATTERN, defaultValue);
  }

  public long getReplQueryTimeout() {
    return getPropertiesFromConfiguration(REPL_QUERY_TIMEOUT_PATTERN, DEFAULT_REPL_TIMEOUT);
  }

  /**
   * Tries to get the specific configuration with the instance name. If not found then tries to
   * find the default set in ambari.properties. If not found then returns the default value passed
   * @param keyPattern Pattern used to generate ambari.properties key
   * @param defaultValue Returned when the value is not found in ambari.properties
   * @return value of the property
   */
  private Long getPropertiesFromConfiguration(String keyPattern, Long defaultValue) {
    Optional<String> value = configuration.get(String.format(keyPattern, DEFAULT_CONFIG));
    if (value.isPresent()) {
      return Long.parseLong(value.get());
    }
    return defaultValue;
  }
}
