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
package com.hortonworks.hivestudio.webapp.registries;

import com.hortonworks.hivestudio.common.util.ApplicationRegistry;
import com.hortonworks.hivestudio.webapp.AppConfiguration;
import com.hortonworks.hivestudio.webapp.resources.AboutResource;
import com.hortonworks.hivestudio.webapp.resources.BundleResource;
import com.hortonworks.hivestudio.webapp.resources.ConnectionResource;
import com.hortonworks.hivestudio.webapp.resources.CountReportResource;
import com.hortonworks.hivestudio.webapp.resources.CsrfTokenResource;
import com.hortonworks.hivestudio.webapp.resources.DDLResource;
import com.hortonworks.hivestudio.webapp.resources.FileOperationResource;
import com.hortonworks.hivestudio.webapp.resources.FileResource;
import com.hortonworks.hivestudio.webapp.resources.FileSystemResource;
import com.hortonworks.hivestudio.webapp.resources.HiveQueryResource;
import com.hortonworks.hivestudio.webapp.resources.JobResource;
import com.hortonworks.hivestudio.webapp.resources.JoinReportResource;
import com.hortonworks.hivestudio.webapp.resources.LdapLoginResource;
import com.hortonworks.hivestudio.webapp.resources.QuerySearchResource;
import com.hortonworks.hivestudio.webapp.resources.ReadReportResource;
import com.hortonworks.hivestudio.webapp.resources.ReplicationDumpResource;
import com.hortonworks.hivestudio.webapp.resources.SavedQueryResource;
import com.hortonworks.hivestudio.webapp.resources.SettingsResource;
import com.hortonworks.hivestudio.webapp.resources.SuggestedSearchesResource;
import com.hortonworks.hivestudio.webapp.resources.UdfResource;
import com.hortonworks.hivestudio.webapp.resources.UploadResource;
import com.hortonworks.hivestudio.webapp.resources.UserResource;

import io.dropwizard.setup.Environment;

/**
 * Register the dropwizard resources here.
 */
public class RestResourcesClassRegistry extends ApplicationRegistry<Class<?>, AppConfiguration> {

  public RestResourcesClassRegistry(AppConfiguration configuration, Environment environment) {
    super(configuration, environment);
  }

  @Override
  protected void register(AppConfiguration configuration, Environment environment) {
    this.add(JobResource.class);
    this.add(DDLResource.class);
    this.add(SettingsResource.class);
    this.add(UdfResource.class);
    this.add(FileResource.class);
    this.add(SavedQueryResource.class);
    this.add(FileOperationResource.class);
    this.add(FileSystemResource.class);
    this.add(UserResource.class);
    this.add(HiveQueryResource.class);
    this.add(QuerySearchResource.class);
    this.add(UploadResource.class);
    this.add(SuggestedSearchesResource.class);
    this.add(JoinReportResource.class);
    this.add(ReadReportResource.class);
    this.add(CountReportResource.class);
    this.add(BundleResource.class);
    this.add(AboutResource.class);
    this.add(ReplicationDumpResource.class);
    this.add(CsrfTokenResource.class);
    this.add(ConnectionResource.class);
    this.add(LdapLoginResource.class);
  }
}
