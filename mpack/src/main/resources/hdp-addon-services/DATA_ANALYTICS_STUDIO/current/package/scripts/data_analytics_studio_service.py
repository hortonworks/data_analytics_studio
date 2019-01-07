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

import os
import das_tools

from ambari_commons import OSCheck
from resource_management.core.exceptions import ComponentIsNotRunning, Fail
from resource_management.core.resources.packaging import Package
from resource_management.core.resources.system import Execute, File
from resource_management.libraries.functions import get_user_call_output
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.security_commons import update_credential_provider_path
from ambari_commons.credential_store_helper import get_password_from_credential_store

def create_credential_providers():
  import params

  if not os.path.exists(params.conf_dir):
    os.makedirs(params.conf_dir)

  for c in ['data_analytics_studio-database', 'data_analytics_studio-properties', 'data_analytics_studio-security-site']:
    update_credential_provider_path(
      params.config['configurations'][c],
      c,
      os.path.join(params.conf_dir, c + '.jceks'),
      params.data_analytics_studio_user,
      params.data_analytics_studio_group
    )

def data_analytics_studio_service(name, action = "start"):
  if name == "data_analytics_studio_webapp":
    data_analytics_studio_postgresql_server_action(action)
    data_analytics_studio_webapp_action(action)
  elif name == "data_analytics_studio_event_processor":
    data_analytics_studio_event_processor_action(action)

def data_analytics_studio_webapp_action(action):
  import params
  if action == 'install':
    return
  
  pid = get_user_call_output.get_user_call_output(format("cat {data_analytics_studio_webapp_pid_file}"),
                                                  user = params.data_analytics_studio_user,
                                                  is_checked_call = False)[1]
  process_id_exists_command = format("ls {data_analytics_studio_webapp_pid_file} > /dev/null 2>&1 && ps -p {pid} >/dev/null 2>&1")
  
  das_home = das_tools.get_das_home()
  cmd = format("source {conf_dir}/das-webapp-env.sh; {das_home}/bin/das-webapp {action}")
  env = {
      "JAVA_HOME": params.java64_home,
      "HADOOP_CONF": params.hadoop_conf_dir
  }
  if action == "start":
    create_credential_providers()
    Execute(cmd,
            not_if = process_id_exists_command,
            environment = env,
            user = params.data_analytics_studio_user)
  else:
    Execute(cmd,
            only_if = process_id_exists_command,
            environment = env,
            user = params.data_analytics_studio_user)

def data_analytics_studio_event_processor_action(action):
  import params
  
  pid = get_user_call_output.get_user_call_output(format("cat {data_analytics_studio_event_processor_pid_file}"),
                                                  user = params.data_analytics_studio_user,
                                                  is_checked_call = False)[1]
  process_id_exists_command = format("ls {data_analytics_studio_event_processor_pid_file} > /dev/null 2>&1 && ps -p {pid} >/dev/null 2>&1")
  
  das_home = das_tools.get_das_home()
  cmd = format("source {conf_dir}/das-event-processor-env.sh; {das_home}/bin/das-event-processor {action}")
  env = {
      "JAVA_HOME": params.java64_home,
      "HADOOP_CONF": params.hadoop_conf_dir
  }
  if action == "start":
    create_credential_providers()
    Execute(cmd,
            not_if = process_id_exists_command,
            environment = env,
            user = params.data_analytics_studio_user)
  else:
    Execute(cmd,
            only_if = process_id_exists_command,
            environment = env,
            user = params.data_analytics_studio_user)

def data_analytics_studio_postgresql_server_action(action):
  import params

  # For custom postgres, everything should be setup by the user.
  if not params.data_analytics_studio_autocreate_db:
    return

  isSystemd = OSCheck.get_os_type() in ["centos", "redhat"] and OSCheck.get_os_version() >= "7.1" or \
              OSCheck.get_os_type() == "fedora" and OSCheck.get_os_version() >= "23" or \
              OSCheck.get_os_type() == "debian" and OSCheck.get_os_version() >= "9.0" or \
              OSCheck.get_os_type() == "ubuntu" and OSCheck.get_os_version() >= "16"

  if action == "install":
    postgresql_server_install(isSystemd)
  else:
    postgresql_server_action(action, isSystemd)

def postgresql_server_install(isSystemd):
  # Get and install the postgresql-9.6 package
  if OSCheck.get_os_type() in ["centos", "redhat", "fedora"]:
    if not(os.path.isfile("/etc/yum.repos.d/pgdg-96-redhat.repo")):
      Package("https://yum.postgresql.org/9.6/redhat/rhel-6-x86_64/pgdg-redhat96-9.6-3.noarch.rpm")

    Package("postgresql96-server")
    Package("postgresql96-contrib")
    if isSystemd:
      if not os.path.exists("/var/lib/pgsql/9.6/data/") or not os.listdir("/var/lib/pgsql/9.6/data/"):
        Execute("/usr/pgsql-9.6/bin/postgresql96-setup initdb")
    else:
      if not os.path.exists("/var/lib/pgsql/9.6/data/") or not os.listdir("/var/lib/pgsql/9.6/data/"):
        Execute("service postgresql-9.6 initdb")
  elif OSCheck.get_os_type() == "debian":
    File(format("/etc/apt/sources.list.d/postgresql.list"),
         content = "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main")
    Execute("wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -")
    Execute("apt-get update")
    Package("postgresql-9.6")
  elif OSCheck.get_os_type() == "ubuntu":
    File(format("/etc/apt/sources.list.d/postgresql.list"),
         content = "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main")
    Execute("wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -")
    Execute("apt-get update")
    Package("postgresql-9.6")

  import data_analytics_studio
  data_analytics_studio.setup_data_analytics_studio_postgresql_server()

  # Initialize the DBs for DAS.
  postgresql_server_action("start", isSystemd)
  data_analytics_studio_initdb()
  postgresql_server_action("stop", isSystemd)

def postgresql_server_action(action, isSystemd):
  if OSCheck.get_os_type() in ["centos", "redhat", "fedora"]:
    pg_ctl = "/usr/pgsql-9.6/bin/pg_ctl"
    pg_data = "/var/lib/pgsql/9.6/data/"
    service_name = "postgresql-9.6"
  elif OSCheck.get_os_type() in ["debian", "ubuntu"]:
    pg_ctl = "/usr/lib/postgresql/9.6/bin/pg_ctl"
    pg_data = "/var/lib/postgresql/9.6/main/"
    service_name = "postgresql"

  if action in ["start", "stop"]:
    if isSystemd:
      Execute(format("{pg_ctl} {action} -D {pg_data}"), user="postgres")
    else:
      Execute(format("service {service_name} {action}"))
  elif action == "status":
    try:
      if isSystemd:
        Execute(format("{pg_ctl} status -D {pg_data} | grep \"server is running\""), user="postgres")
      else:
        Execute(format("service {service_name} status | grep running"))
    except Fail as err:
      # raise ComponentIsNotRunning(), let webapp override this.
      return

def data_analytics_studio_initdb():
    import params
    create_credential_providers()
    dbPassword=get_password_from_credential_store("data_analytics_studio_database_password", params.das_credential_provider_paths, params.das_credential_store_class_path, params.java64_home, params.jdk_location)
    pg_cmd = """
        psql -tc \"SELECT 1 FROM pg_database WHERE datname = '{data_analytics_studio_database_name}'\" | grep 1 || (
        psql -c \"CREATE ROLE {data_analytics_studio_database_username} WITH LOGIN PASSWORD '{password}';\" &&
        psql -c \"ALTER ROLE {data_analytics_studio_database_username} SUPERUSER;\" &&
        psql -c \"ALTER ROLE {data_analytics_studio_database_username} CREATEDB;\" &&
        psql -c \"CREATE DATABASE {data_analytics_studio_database_name};\" &&
        psql -c \"GRANT ALL PRIVILEGES ON DATABASE {data_analytics_studio_database_name} TO {data_analytics_studio_database_username};\")
    """
    Execute(format(pg_cmd, password=dbPassword), user = "postgres")

