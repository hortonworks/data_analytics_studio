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
package com.hortonworks.hivestudio.common;

public interface Constants {
  //ambari
  String AMBARI_HIVE_SERVICE_NAME = "HIVE";
  String AMBARI_HIVESERVER_COMPONENT_NAME = "HIVE_SERVER";

//  config file names
  String CONFIG_FILE_NAME = "das.conf";
  String HIVE_SITE_CONFIG_FILE_NAME = "das-hive-site.conf";
  String HIVE_INTERACTIVE_SITE_CONFIG_FILE_NAME = "das-hive-interactive-site.conf";

  String DEFAULT_CONFIG_DIR = "/etc/das/conf/";
  String DEFAULT_TEMPLATE_DIR = DEFAULT_CONFIG_DIR + "/template";

//  application related
  String DAS = "das";
  String APPLICATION_NAME = "application.name";
  String DEFAULT_VERSION_VAL = "Not Available!";
  String APPLICATION_VERSION = "application.version";

  String CONFIG_FOLDER_ENV_NAME = "das.config.dir";
  String JOBS_DIR = "das.jobs.dir";

  String HIVESTUDIO_API_URL_PROPERTY="das.api.url";
  String DEFAULT_HIVESTUDIO_API_URL = "http://localhost:8080/api";

  String HIVESTUDIO_DEBUG_BUNDLER_EXTRACT_LOGS = "hivestudio.debug.bundler.extract.logs";
  String DEFAULT_HIVESTUDIO_DEBUG_BUNDLER_EXTRACT_LOGS = "true";

  // REST end points
  String REPLICATION_DUMP_URL = "replicationDump";
  String BOOTSTRAP_DUMP_URL = "bootstrap";
  String INCREMENTAL_DUMP_URL = "incremental";
  String FULL_BOOTSTRAP_DUMP_URL = "full_bootstrap";
  String FULL_INCREMENTAL_DUMP_URL = "full_incremental";
  String DATABASE_NAME_PARAM = "databaseName";
  String LAST_REPLICATION_ID_PARAM = "lastReplicationId";
  String MAX_NUMBER_OF_EVENTS = "maxNumberOfEvents";

  //  hdfs
  String DEFAULT_FS_KEY = "fs.defaultFS";
  String DEFAULT_FS_VALUE = "hdfs://localhost:8020";

  //  hive
  String HIVE_SESSION_PARAMS_KEY = "hive.session.params";
  String HIVE_SESSION_PARAMS_VALUE = "";// "transportMode=http;httpPath=cliservice";
  String HIVE_DYNAMIC_SERVICE_DISCOVERY_KEY = "hive.server2.support.dynamic.service.discovery";
  String HIVE_DYNAMIC_SERVICE_DISCOVERY_KEY_VALUE = "true";

  String HIVE_INTERACTIVE_DYNAMIC_SERVICE_DISCOVERY_KEY = "hive.server2.support.dynamic.service.discovery";
  String HIVE_INTERACTIVE_DYNAMIC_SERVICE_DISCOVERY_KEY_VALUE = "true";

  String HIVE_ZOOKEEPER_QUORUM_KEY = "hive.zookeeper.quorum";
  String HIVE_DO_AS_KEY = "hive.server2.enable.doAs";
  String HIVE_ZOOKEEPER_QUORUM_VALUE = "localhost:2181";
  String HIVE_DO_AS_VALUE = "false";

  String HIVE_ZOOKEEPER_QUORUM_NAMESPACE_KEY = "hive.server2.zookeeper.namespace";
  String HIVE_ZOOKEEPER_QUORUM_NAMESPACE_VALUE = "hiveserver2";

  String BINARY_PORT_KEY = "hive.server2.thrift.port";
  String HIVE_JDBC_URL_KEY = "hive.jdbc.url";
  String HIVE_SESSION_PARAMS = "hive.session.params";
  String HIVE_LDAP_CONFIG = "hive.ldap.configured";
  String HIVE_AUTH_MODE = "hive.server2.authentication";
  String HTTP_PORT_KEY = "hive.server2.thrift.http.port";
  String HIVE_TRANSPORT_MODE_KEY = "hive.server2.transport.mode";
  String HTTP_PATH_KEY = "hive.server2.thrift.http.path";
  String HS2_PROXY_USER = "hive.server2.proxy.user";
  String USE_HIVE_INTERACTIVE_MODE = "use.hive.interactive.mode";


  // DB related
  String DATABASE_SCHEMA = "das";
  String TABLES_TABLE_NAME = "tables";
  String COLUMNS_TABLE_NAME = "columns";
  String DATABASES_TABLE_NAME = "databases";
  String TABLE_PARTITION_INFO_TABLE_NAME = "table_partition_info";


  String HIVE_METASTORE_LOCATION_KEY = "hive.metastore.warehouse.dir";
  String HIVE_METASTORE_LOCATION_KEY_VIEW_PROPERTY = HIVE_METASTORE_LOCATION_KEY;
  String HIVE_DEFAULT_METASTORE_LOCATION = "/apps/hive/warehouse";
  String HIVE_DEFAULT_DB = "default";


  // Product constants
  String ENV_VERSION_RESOURCE = "env-version.sh";
  String VERSION_PREFIX = "DAS_VERSION";
  String PRODUCT_NAME = "DATA ANALYTICS STUDIO";
  String CLUSTER_ID = "cluster_id";

  String XSRF_HEADER = "X-Requested-By";
  String CONNECTION_URL_KEY = "connection-url";
  String SESSION_USER_KEY = "username";
}
