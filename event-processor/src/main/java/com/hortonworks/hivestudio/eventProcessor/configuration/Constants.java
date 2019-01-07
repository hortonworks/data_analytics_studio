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
package com.hortonworks.hivestudio.eventProcessor.configuration;

public interface Constants {
  String SCAN_FOLDER_DELAY_MILLIS = "scan.folder.delay.millis";
  long DEFAULT_SCAN_FOLDER_DELAY_MILLIS = 2000l;

  String AUTO_CLOSE_MAX_WAIT_TIME_MILLIS = "autoclose.max.wait.time.millis";
  long DEFAULT_AUTO_CLOSE_MAX_WAIT_TIME_MILLIS = 4 * 24 * 3600 * 1000l; // 4 days

  String HDFS_MAX_SYNC_WAIT_TIME_MILLIS = "hdfs.max.sync.wait.time.millis";
  long DEFAULT_HDFS_MAX_SYNC_WAIT_TIME_MILLIS = 2 * 60 * 1000l; // 2 minutes.

  String EVENT_PIPELINE_MAX_PARALLELISM = "event.pipeline.max.parallelism";
  int DEFAULT_EVENT_PIPELINE_MAX_PARALLELISM = 50;

  //meta refresher
  String ENABLE_REFRESH_META_INFO_SERVICE = "meta.info.sync.service.enabled";
  String META_INFO_SYNC_SERVICE_DELAY_MILLIS = "meta.info.sync.service.delay.millis";
  Long DEFAULT_META_INFO_SYNC_SERVICE_DELAY_MILLIS = 1 * 60 * 1000l;
  Boolean ENABLE_REFRESH_META_INFO_SERVICE_DEFAULT = Boolean.TRUE;
  String TABLE_COMMENT_PROPERTY = "COMMENT";
  String ALL_DB_STAR = "`*`";
  String METADATA = "_metadata";
  String CONSTRAINTS = "_constraints";
  String FUNCTIONS = "_functions";
  String DUMPMETADATA = "_dumpmetadata";
}
