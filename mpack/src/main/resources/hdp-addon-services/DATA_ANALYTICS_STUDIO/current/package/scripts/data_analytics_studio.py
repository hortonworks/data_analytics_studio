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

import glob
import os

from ambari_commons import OSCheck
from resource_management.core.resources.system import Execute, File, Directory
from resource_management.core.source import InlineTemplate, Template
from resource_management.libraries.functions.format import format
from resource_management.libraries.resources.properties_file import PropertiesFile

def data_analytics_studio(name = None):
  import params

  if name == "data_analytics_studio_webapp":
    setup_data_analytics_studio_postgresql_server()
    setup_data_analytics_studio_configs()
    setup_data_analytics_studio_webapp()
  elif name == "data_analytics_studio_event_processor":
    setup_data_analytics_studio_configs()
    setup_data_analytics_studio_event_processor()

def setup_data_analytics_studio_webapp():
  import params

  File(params.das_webapp_json,
       content = InlineTemplate(params.das_webapp_properties_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

  File(params.das_webapp_env_sh,
       content = InlineTemplate(params.das_webapp_env_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

def setup_data_analytics_studio_event_processor():
  import params

  File(params.das_event_processor_json,
       content = InlineTemplate(params.das_event_processor_properties_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

  File(params.das_event_processor_env_sh,
       content = InlineTemplate(params.das_event_processor_env_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )

def setup_data_analytics_studio_configs():
  import params

  # TODO: revisit ownership and permission for all the files here. I think all
  # should be owned by root and readable by all. Have to find policy to store
  # password in conf file.

  # Bug in ambari mkdirs, it does not set the mode all the way.
  Directory(os.path.dirname(params.conf_dir),
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)

  Directory(params.conf_dir,
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)

  Directory(params.data_analytics_studio_pid_dir,
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)

  Directory(params.data_analytics_studio_log_dir,
          owner = params.data_analytics_studio_user,
          create_parents = True,
          mode = 0755)

  File(params.das_conf,
       content = InlineTemplate(params.das_conf_content),
       owner = params.data_analytics_studio_user,
       mode = 0400
  )
  
  PropertiesFile(params.das_hive_site_conf,
                 properties = params.das_hive_site_conf_dict,
                 owner = params.data_analytics_studio_user,
                 mode = 0400
  )
  
  PropertiesFile(params.das_hive_interactive_site_conf,
                 properties = params.das_hive_interactive_site_conf_dict,
                 owner = params.data_analytics_studio_user,
                 mode = 0400
  )

def setup_data_analytics_studio_postgresql_server():
  import params

  if not params.data_analytics_studio_autocreate_db:
    return

  if OSCheck.get_os_type() in ["centos", "redhat", "fedora"]:
    pgpath = "/var/lib/pgsql/9.6/data"
  elif OSCheck.get_os_type() in ["debian", "ubuntu"]:
    pgpath = "/var/lib/postgresql/9.6/main"

  File(format("{pgpath}/pg_hba.conf"),
       content = InlineTemplate(params.data_analytics_studio_postgresql_pg_hba_conf_content),
       owner = "postgres",
       group = "postgres",
       mode = 0600)

  File(format("{pgpath}/postgresql.conf"),
       content = InlineTemplate(params.data_analytics_studio_postgresql_postgresql_conf_content),
       owner = "postgres",
       group = "postgres",
       mode = 0600)
