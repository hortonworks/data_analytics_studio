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

package com.hortonworks.hivestudio.debugBundler.framework;

import com.google.inject.Injector;
import com.hortonworks.hivestudio.debugBundler.source.HiveStudioArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.LlapDeamonLogsArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.LlapDeamonLogsListArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.SliderAMInfoArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.SliderAMLogsArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.SliderAMLogsListArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.SliderInstanceJmx;
import com.hortonworks.hivestudio.debugBundler.source.SliderInstanceStack;
import com.hortonworks.hivestudio.debugBundler.source.SliderStatusArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.TezAMInfoArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.TezAMLogsArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.TezAMLogsListArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.TezHDFSArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.TezTasksLogsArtifacts;
import com.hortonworks.hivestudio.debugBundler.source.TezTasksLogsListArtifacts;

public enum ArtifactSourceType implements ArtifactSourceCreator {

  // From Hive Studio - Needs Query ID
  HIVE_STUDIO(HiveStudioArtifacts.class),

  TEZ_HDFS(TezHDFSArtifacts.class),

  // From Tez AM - Needs App ID
  TEZ_AM_INFO(TezAMInfoArtifacts.class),
  TEZ_AM_LOG_INFO(TezAMLogsListArtifacts.class),
  TEZ_AM_LOGS(TezAMLogsArtifacts.class),
  // TEZ_AM_JMX(DummyArtifacts.class),
  // TEZ_AM_STACK(DummyArtifacts.class),

  // From Tez AHS - Needs Node ID / Container ID list.
  TEZ_TASK_LOGS_INFO(TezTasksLogsListArtifacts.class),
  TEZ_TASK_LOGS(TezTasksLogsArtifacts.class),

  // LLAP
  LLAP_DEAMON_LOGS_INFO(LlapDeamonLogsListArtifacts.class),
  LLAP_DEAMON_LOGS(LlapDeamonLogsArtifacts.class),
  //SLIDER_STATUS(SliderStatusArtifacts.class),
  SLIDER_INSTANCE_JMX(SliderInstanceJmx.class),
  SLIDER_INSTANCE_STACK(SliderInstanceStack.class),
  SLIDER_AM_INFO(SliderAMInfoArtifacts.class),
  SLIDER_AM_LOG_INFO(SliderAMLogsListArtifacts.class),
  SLIDER_AM_LOGS(SliderAMLogsArtifacts.class),
  // TEZ_CONFIG(DummyArtifacts.class),
  // TEZ_HIVE2_CONFIG(DummyArtifacts.class),

  // From ATS - Need DAG ID
  // ATS_DOMAIN(ATSDomainArtifacts.class), // For ACLs
  // DAG_ATS(DAGATSArtifact.class),
  // TEZ_ATS(TezATSArtifacts.class),
  // HIVE_ATS(HiveATSArtifacts.class),
  // HIVE_CONFIG(DummyArtifacts.class),
  // HIVE2_CONFIG(DummyArtifacts.class),
  // HADOOP_CONFIG(DummyArtifacts.class),
  // HIVESERVER2_LOG(DummyArtifacts.class),
  ;

  private final Class<? extends ArtifactSource> sourceClass;
  private ArtifactSourceType(Class<? extends ArtifactSource> sourceClass) {
    this.sourceClass = sourceClass;
  }

  public ArtifactSource getSource(Injector injector) {
    return injector.getInstance(sourceClass);
  }
}
