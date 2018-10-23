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
package com.hortonworks.hivestudio.common.config;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;


@Slf4j   // in JUnit test cases logger can be setup through lombok annotation.
public class ConfigurationTest {

  public static final String KEY_1 = "key1";
  public static final String VALUE_1 = "value1";
  public static final String DEFAULT_VALUE = "default_value";
  public static final String KEY_2 = "key2";
  public static final String VALUE_2 = "value2";

  public static final String KEY_3 = "key3";


  @Test
  public void get() throws Exception {
    log.info("Testing normal working of Configuration get method");
    Properties properties = new Properties();
    properties.setProperty(KEY_1, VALUE_1);
    properties.setProperty(KEY_2, VALUE_2);
    Configuration configuration = new Configuration(properties);

    Optional<String> optionalValue1 = configuration.get(KEY_1);
    Assert.assertTrue("check if value is present in returned optional.", optionalValue1.isPresent());
    Assert.assertEquals("check returned value for valid key.", VALUE_1, optionalValue1.get());

    Optional<String> optionalValue2 = configuration.get(KEY_2);
    Assert.assertTrue("check if value is present in optional value2.", optionalValue2.isPresent());
    Assert.assertEquals("check returned value for another valid key.", VALUE_2, optionalValue2.get());

    Optional<String> optionalValue3 = configuration.get(KEY_3);
    Assert.assertFalse("check if optional is absent for non existent key", optionalValue3.isPresent());
  }

  @Test
  public void getWithDefault() throws Exception {
    log.info("Testing normal working of getWithDefault Method");
    Properties properties = new Properties();
    properties.setProperty(KEY_1, VALUE_1);
    properties.setProperty(KEY_2, VALUE_2);
    Configuration configuration = new Configuration(properties);

    String value1 = configuration.get(KEY_1, DEFAULT_VALUE);
    Assert.assertEquals("check returned value for valid key.", VALUE_1, value1);

    String value2 = configuration.get(KEY_2, DEFAULT_VALUE);
    Assert.assertEquals("check returned value for another valid key.", VALUE_2, value2);

    String defaultValue = configuration.get(KEY_3, DEFAULT_VALUE);
    Assert.assertEquals("check if default value is returned for non existent key.", DEFAULT_VALUE, defaultValue);
  }

}