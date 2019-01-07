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
package com.hortonworks.hivestudio.common.util;

import com.google.common.base.Strings;
import com.hortonworks.hivestudio.common.Constants;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Singleton
public class PropertyUtils {
  public void readPropertyFile(Properties properties, String configFolder, String fileName) {
    try {
      // load properties defined in system path
      String folderPath = configFolder;
      if(Strings.isNullOrEmpty(folderPath)){
        log.info("no configuration folder was provided. Using default configFolder {}.", Constants.DEFAULT_CONFIG_DIR);
        folderPath = Constants.DEFAULT_CONFIG_DIR;
      }
      File configFileWithPath = new File(new File(folderPath), fileName);
      if(configFileWithPath.exists()){
        log.info("loading configurations from file {}", configFileWithPath.toString());
        properties.load(new FileInputStream(configFileWithPath));
      }else{
        // else load properties in class path
        log.info("file not found at path of {}, trying to load file {} from classpath.", configFileWithPath, fileName);
        InputStream configsInClassPath = this.getClass().getResourceAsStream(fileName);
        if(null != configsInClassPath){
          log.info("loading configurations found in class path.");
          properties.load(configsInClassPath);
        }else{
          log.error("could not load properties for {}", fileName);
        }
      }
    } catch (IOException e) {
      log.error("Error while loading configs from file. Will return with default configs from code.", e);
    }
  }
}
