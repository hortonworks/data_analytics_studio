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

import com.hortonworks.hivestudio.common.dto.DumpInfo;
import com.hortonworks.hivestudio.common.dto.WarehouseDumpInfo;
import com.hortonworks.hivestudio.hive.HiveContext;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class ReplicationDumpService {

  private DDLProxy ddlProxy;

  @Inject
  public ReplicationDumpService(DDLProxy ddlProxy){
    this.ddlProxy = ddlProxy;
  }

  public DumpInfo createBootstrapDump(HiveContext hiveContext, String databaseName){
    return ddlProxy.createBootstrapDump(hiveContext, databaseName);
  }

  public DumpInfo createIncrementalDump(HiveContext hiveContext, String databaseName, Integer lastReplicationId){
    return ddlProxy.createIncrementalDump(hiveContext, databaseName, lastReplicationId);
  }

  public WarehouseDumpInfo createWarehouseBootstrapDump(HiveContext hiveContext){
    return ddlProxy.createWarehouseBootstrapDump(hiveContext);
  }

  public WarehouseDumpInfo createWarehouseIncrementalDump(HiveContext hiveContext, Integer lastReplicationId, Integer maxNumberOfEvents){
    return ddlProxy.createWarehouseIncrementalDump(hiveContext, lastReplicationId, maxNumberOfEvents);
  }
}
