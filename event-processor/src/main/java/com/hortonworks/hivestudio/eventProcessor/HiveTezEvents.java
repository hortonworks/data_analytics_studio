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
package com.hortonworks.hivestudio.eventProcessor;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveTezEvents {


  public static String getSubmitEvent() {
    return readFile("hive-events/submit.json");
  }

  public static String getCompletedEvent() {
    return readFile("hive-events/completed.json");
  }

  public static String getTezDagSubmittedEvent() {
    return readFile("tez-events/dag-submitted.json");
  }

  public static String getTezDagInitializedEvent() {
    return readFile("tez-events/dag-initialized.json");
  }

  public static String getTezDagStartedEvent() {
    return readFile("tez-events/dag-started.json");
  }

  public static String getTezDagFinshedEvent() {
    return readFile("tez-events/dag-finished.json");
  }

  public static String getTezVertexInitializedEvent() {
    return readFile("tez-events/vertex-initialized.json");
  }

  public static String getTezVertexStartedEvent() {
    return readFile("tez-events/vertex-started.json");
  }


  public static String getTezVertexConfigureDoneEvent() {
    return readFile("tez-events/vertex-configure-done.json");
  }

  public static String getTezVertexFinishedEvent() {
    return readFile("tez-events/vertex-finished.json");
  }

  private static String readFile(String fileName) {
    try {
      return FileUtils.readFileToString(new File(fileName));
    } catch (IOException e) {
      log.error("Failed to read the submit event file.", e);
      throw new RuntimeException(e);
    }
  }
}
