#!/usr/bin/env python
"""
  HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES

  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
"""

import functools
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.script.script import Script

import status_params

config = Script.get_config()

if 'hostLevelParams' in config and 'java_home' in config['hostLevelParams']:
    java64_home = config["hostLevelParams"]["java_home"]
    jdk_location = config["hostLevelParams"]["jdk_location"]
else:
    java64_home = config['ambariLevelParams']['java_home']
    jdk_location = config["ambariLevelParams"]["jdk_location"]

conf_dir = "/etc/das/conf"

# env variables
data_analytics_studio_pid_dir = status_params.data_analytics_studio_pid_dir
data_analytics_studio_webapp_pid_file = status_params.data_analytics_studio_webapp_pid_file
data_analytics_studio_event_processor_pid_file = status_params.data_analytics_studio_event_processor_pid_file
data_analytics_studio_webapp_additional_classpath = default("/configurations/data_analytics_studio-env/webapp_additional_classpath", "")
data_analytics_studio_ep_additional_classpath = default("/configurations/data_analytics_studio-env/ep_additional_classpath", "")

data_analytics_studio_log_dir = default("/configurations/data_analytics_studio-env/data_analytics_studio_log_dir", "/var/log/das")
data_analytics_studio_user = default("/configurations/hive-env/hive_user", "hive")
#TODO: Find of this is the right group to use as this could give access to many
data_analytics_studio_group = config['configurations']['cluster-env']['user_group']

data_analytics_studio_webapp_jvm_opts = format(default("/configurations/data_analytics_studio-env/webapp_jvm_opts", ""))
data_analytics_studio_ep_jvm_opts = format(default("/configurations/data_analytics_studio-env/ep_jvm_opts", ""))

# configuration files
das_webapp_json = format("{conf_dir}/das-webapp.json")
das_webapp_env_sh = format("{conf_dir}/das-webapp-env.sh")
das_event_processor_json = format("{conf_dir}/das-event-processor.json")
das_event_processor_env_sh = format("{conf_dir}/das-event-processor-env.sh")
das_conf = format("{conf_dir}/das.conf")
das_hive_site_conf = format("{conf_dir}/das-hive-site.conf")
das_hive_interactive_site_conf = format("{conf_dir}/das-hive-interactive-site.conf")

# contents for file creations
data_analytics_studio_hive_session_params = config["configurations"]["data_analytics_studio-properties"]["hive_session_params"]
das_conf_content = config["configurations"]["data_analytics_studio-properties"]["content"]
das_webapp_env_content = config["configurations"]["data_analytics_studio-webapp-env"]["content"]
das_webapp_properties_content = config["configurations"]["data_analytics_studio-webapp-properties"]["content"]
das_event_processor_env_content = config["configurations"]["data_analytics_studio-event_processor-env"]["content"]
das_event_processor_properties_content = config["configurations"]["data_analytics_studio-event_processor-properties"]["content"]
data_analytics_studio_postgresql_postgresql_conf_content = config["configurations"]["data_analytics_studio-database"]["postgresql_conf_content"]
data_analytics_studio_postgresql_pg_hba_conf_content = config["configurations"]["data_analytics_studio-database"]["pg_hba_conf_content"]

# properties
data_analytics_studio_ssl_enabled = default("/configurations/data_analytics_studio-security-site/ssl_enabled", False)
das_credential_provider_paths = format("jceks://file{conf_dir}/data_analytics_studio-database.jceks,jceks://file{conf_dir}/data_analytics_studio-properties.jceks,jceks://file{conf_dir}/data_analytics_studio-security-site.jceks")

data_analytics_studio_webapp_server_protocol = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_server_protocol", "http")
data_analytics_studio_webapp_server_host = default("/clusterHostInfo/data_analytics_studio_webapp_hosts", ["localhost"])[0]
data_analytics_studio_webapp_server_port = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_server_port", "30800")
data_analytics_studio_webapp_server_url = format("{data_analytics_studio_webapp_server_protocol}://{data_analytics_studio_webapp_server_host}:{data_analytics_studio_webapp_server_port}/api/")
data_analytics_studio_webapp_admin_port = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_admin_port", "30801")
data_analytics_studio_webapp_smartsense_id = default("/configurations/data_analytics_studio-webapp-properties/data_analytics_studio_webapp_smartsense_id", "")
data_analytics_studio_admin_users = default("/configurations/data_analytics_studio-security-site/admin_users", "")
data_analytics_studio_webapp_auth_enabled = str(default("/configurations/data_analytics_studio-security-site/authentication_enabled", "false")).lower()
data_analytics_studio_webapp_service_auth_type = default("/configurations/data_analytics_studio-security-site/service_authentication_type", "")
data_analytics_studio_webapp_service_keytab = default("/configurations/data_analytics_studio-webapp-properties/service_keytab", "")
data_analytics_studio_webapp_service_principal = default("/configurations/data_analytics_studio-webapp-properties/service_principal", "")
data_analytics_studio_webapp_knox_sso_enabled = str(default("/configurations/data_analytics_studio-security-site/knox_sso_enabled", "false")).lower()
data_analytics_studio_webapp_knox_sso_url = default("/configurations/data_analytics_studio-security-site/knox_sso_url", "")
data_analytics_studio_webapp_knox_useragent = default("/configurations/data_analytics_studio-security-site/knox_useragent", "Mozilla,Chrome")
data_analytics_studio_webapp_knox_publickey = default("/configurations/data_analytics_studio-security-site/knox_publickey", "")
data_analytics_studio_webapp_knox_cookiename = default("/configurations/data_analytics_studio-security-site/knox_cookiename", "hadoop-jwt")
data_analytics_studio_webapp_knox_url_query_param = default("/configurations/data_analytics_studio-security-site/knox_url_query_param", "originalUrl")
data_analytics_studio_webapp_keystore_file = default("/configurations/data_analytics_studio-security-site/webapp_keystore_file", "")
data_analytics_studio_webapp_keystore_password = default("/configurations/data_analytics_studio-security-site/webapp_keystore_password", "")

data_analytics_studio_event_processor_server_protocol = default("/configurations/data_analytics_studio-event_processor-properties/data_analytics_studio_event_processor_server_protocol", "http")
data_analytics_studio_event_processor_server_port = default("/configurations/data_analytics_studio-event_processor-properties/data_analytics_studio_event_processor_server_port", "30900")
data_analytics_studio_event_processor_admin_server_port = default("/configurations/data_analytics_studio-event_processor-properties/data_analytics_studio_event_processor_admin_server_port", "30901")
data_analytics_studio_event_processor_keystore_file = default("/configurations/data_analytics_studio-security-site/event_processor_keystore_file", "")
data_analytics_studio_event_processor_keystore_password = default("/configurations/data_analytics_studio-security-site/event_processor_keystore_password", "")

data_analytics_studio_autocreate_db = default("/configurations/data_analytics_studio-database/das_autocreate_db", True)
if data_analytics_studio_autocreate_db:
  data_analytics_studio_database_host = data_analytics_studio_webapp_server_host
else:
  data_analytics_studio_database_host = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_host", "")
data_analytics_studio_database_port = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_port", "5432")
data_analytics_studio_database_name = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_name", "das")
data_analytics_studio_database_username = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_username", "das")
data_analytics_studio_database_password = default("/configurations/data_analytics_studio-database/data_analytics_studio_database_password", "das")
data_analytics_studio_database_jdbc_url = format("jdbc:postgresql://{data_analytics_studio_database_host}:{data_analytics_studio_database_port}/{data_analytics_studio_database_name}")

das_credential_store_class_path = default("/configurations/data_analytics_studio-database/credentialStoreClassPath", "/var/lib/ambari-agent/cred/lib/*")

# das-hive-site.conf, das-hive-interactive-site.conf
hive_server2_support_dynamic_service_discovery = str(default("/configurations/hive-site/hive.server2.support.dynamic.service.discovery", "True")).lower()
hive_server2_zookeeper_namespace = default("/configurations/hive-site/hive.server2.zookeeper.namespace", "hiveserver2")
hive_interactive_server_zookeeper_namespace = default("/configurations/hive-interactive-site/hive.server2.zookeeper.namespace", "hiveserver2-interactive")
hive_zookeeper_quorum = default("/configurations/hive-site/hive.zookeeper.quorum", "")
hive_doAs = default("/configurations/hive-site/hive.server2.enable.doAs", "")

das_hive_site_conf_dict = {
  "hive.server2.zookeeper.namespace": hive_server2_zookeeper_namespace,
  "hive.server2.support.dynamic.service.discovery": hive_server2_support_dynamic_service_discovery,
  "hive.zookeeper.quorum": hive_zookeeper_quorum,
  "hive.server2.enable.doAs": hive_doAs
}

das_hive_interactive_site_conf_dict = {
  "hive.server2.support.dynamic.service.discovery": hive_server2_support_dynamic_service_discovery,
  "hive.server2.zookeeper.namespace": hive_interactive_server_zookeeper_namespace,
  "hive.zookeeper.quorum": hive_zookeeper_quorum
}

# das-event-processor.json
hive_metastore_warehouse_dir = config['configurations']['hive-site']["hive.metastore.warehouse.dir"]
hive_metastore_warehouse_external_dir = config['configurations']['hive-site']["hive.metastore.warehouse.external.dir"]
data_analytics_studio_event_processor_hive_base_dir = format(config["configurations"]["hive-site"]["hive.hook.proto.base-directory"])
data_analytics_studio_event_processor_tez_base_dir = format(config["configurations"]["tez-site"]["tez.history.logging.proto-base-dir"])
data_analytics_studio_event_processor_auth_enabled = str(default("/configurations/data_analytics_studio-security-site/authentication_enabled", "false")).lower()
data_analytics_studio_event_processor_service_auth_type = default("/configurations/data_analytics_studio-security-site/service_authentication_type", "")
data_analytics_studio_event_processor_service_keytab = default("/configurations/data_analytics_studio-event_processor-properties/service_keytab", "")
data_analytics_studio_event_processor_service_principal = default("/configurations/data_analytics_studio-event_processor-properties/service_principal", "")

#create partial functions with common arguments for every HdfsResource call
#to create/delete/copyfromlocal hdfs directories/files we need to call params.HdfsResource in code
hdfs_user = status_params.hdfs_user
security_enabled = config['configurations']['cluster-env']['security_enabled']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
hadoop_bin_dir = stack_select.get_hadoop_dir("bin")
hadoop_conf_dir = conf_select.get_hadoop_conf_dir()
hdfs_principal_name = default('/configurations/hadoop-env/hdfs_principal_name', None)
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']
if 'hostLevelParams' in config and 'not_managed_hdfs_path_list' in config['hostLevelParams']:
    not_managed_resources = get_not_managed_resources()
elif 'clusterLevelParams' in config and 'not_managed_hdfs_path_list' in config['clusterLevelParams']:
    not_managed_resources = get_not_managed_resources()
else:
    not_managed_resources = ''
dfs_type = default("/commandParams/dfs_type", "")

HdfsResource = functools.partial(
  HdfsResource,
  user = hdfs_user,
  hdfs_resource_ignore_file = "/var/lib/ambari-agent/data/.hdfs_resource_ignore",
  security_enabled = security_enabled,
  keytab = hdfs_user_keytab,
  kinit_path_local = kinit_path_local,
  hadoop_bin_dir = hadoop_bin_dir,
  hadoop_conf_dir = hadoop_conf_dir,
  principal_name = hdfs_principal_name,
  hdfs_site = hdfs_site,
  default_fs = default_fs,
  immutable_paths = not_managed_resources,
  dfs_type = dfs_type
)
